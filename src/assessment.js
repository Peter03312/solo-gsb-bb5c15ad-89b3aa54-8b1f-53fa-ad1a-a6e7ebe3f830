'use strict';

/**
 * 学员考核（仅命名演练副本；原站场不能开考）。
 *
 * 语义（与需求一一对应）：
 *  - 管理员为当前运行中的命名演练副本指定一名学员与【按顺序完成】的进路目标，
 *    开考时在同一事务内固定目标列表与起始状态（记录当时最大进路实例编号作为
 *    水位 start_instance_watermark，仅统计开考后的新进路实例）。
 *  - 原站场不能开考；已暂停副本不能开考；同一副本同时只允许一场进行中（active）
 *    的考核（部分唯一索引 idx_exam_active_once 兜底，重复开考 409 拒绝）。
 *  - 只考核开考后归属该学员的新进路实例：
 *      · 成功开放（直接开放、等待申请自动兑现产生）时，由
 *        onInstanceOpenedInTxn 判定：进路与当前待达成目标相符则"占住"该目标
 *        （exam_targets.instance_id），顺序外的成功开放记为偏差 exam_deviations；
 *      · 成功占住目标的实例通过逐段走行【正常解除】（末段清空自动解除；人工事件
 *        走行或定时走行计划注入都走同一套走行规则）时，由
 *        onInstanceNormalReleasedInTxn 把目标标记 completed、记录完成时刻——
 *        等待申请自动兑现开放、定时走行完成都计入；
 *      · 幂等重传不重复计数（目标实例唯一索引 + 完成前状态判断）；
 *      · 人工释放（提前/兜底解除）与延时取消到期解除都不是"逐段走行正常解除"，
 *        不算完成：占住目标的实例被这样解除时，目标退回未占住，学员可再次排路
 *        完成该目标（是否重新占住取决于当时是否仍为当前目标）；
 *      · 学员在顺序外成功开放的进路记为偏差，之后完成也不补算（偏差实例解除时
 *        不会推进任何目标）；其他人（opened_by 非学员）的操作不计入；
 *      · 联锁规则不因考核改变：钩子只记录、不改变开放/走行/解锁结果。
 *  - 有效用时：暂停冻结。run_started_at 记录当前运行段起点；暂停时把本段时长并入
 *    active_elapsed_ms 并清空起点，恢复时重设；交卷/终止时固化到 final_elapsed_ms。
 *  - 检查点回退或站场重置：终止进行中（active）的考核并记录原因；已交卷（submitted）
 *    成绩保留。回退后盘面被整体替换：终止后旧考核不再观察新盘面（新考核重新记水位）。
 *  - 交卷（submitExam）返回逐项目标状态、偏差与有效用时；刷新/重启后仍可查询。
 *    未知进路（配置目标时）、非指定学员交卷、已终止考核交卷、重复开考一律拒绝；
 *    已交卷后重复交卷幂等返回原成绩。
 *
 * 所有钩子都在调用方事务内执行（InTxn），保证"实例开放/解除"与"考核记账"
 * 同进同退；钩子内部捕获自身异常不改变联锁主流程（考核记账失败不应让一次合法
 * 排路失败——但正常路径下两者同事务，错误会整体回滚）。
 */

const { getDb } = require('./db');
const { InterlockError } = require('./interlocking');

const NAME_MAX = 30;

/* ------------------------------ 工具 ------------------------------ */

function getExamRow(id) {
  return getDb().prepare(`SELECT * FROM exams WHERE id=?`).get(id);
}

function activeExamRow() {
  return getDb().prepare(`SELECT * FROM exams WHERE status='active' ORDER BY id DESC LIMIT 1`).get();
}

/** 当前有效用时（毫秒）：终态返回固化值；进行中=已冻结累计 + 当前运行段。 */
function elapsedMsOf(row, now = Date.now()) {
  if (row.status !== 'active') return Number(row.final_elapsed_ms || 0);
  return Number(row.active_elapsed_ms || 0) +
    (row.run_started_at != null ? Math.max(0, now - Number(row.run_started_at)) : 0);
}

/* ------------------------------ 开考 ------------------------------ */

/**
 * 开考（管理员）。同事务固定目标与起始状态。
 * @param {string} student 学员姓名
 * @param {string[]} routeIds 按顺序完成的进路目标（进路编号列表）
 * @param {string} operator 开考管理员
 */
function startExam(student, routeIds, operator) {
  const studentName = String(student == null ? '' : student).trim();
  if (!studentName || studentName.length > NAME_MAX) {
    throw new InterlockError(400, 'EXAM_STUDENT_INVALID',
      `学员姓名需为 1~${NAME_MAX} 字非空白字符`);
  }
  const ids = Array.isArray(routeIds) ? routeIds.map((r) => String(r == null ? '' : r).trim()) : [];
  if (ids.length === 0) {
    throw new InterlockError(400, 'EXAM_TARGETS_EMPTY', '开考至少需要指定一个按顺序完成的进路目标');
  }
  if (ids.some((r) => !r)) {
    throw new InterlockError(400, 'EXAM_TARGET_INVALID', '进路目标编号不能为空');
  }

  const db = getDb();
  const now = Date.now();

  const examId = db.transaction(() => {
    // 重复开考：同副本已有进行中考核 → 拒绝（幂等开考不适用，须先交卷或等待终止）
    const existing = activeExamRow();
    if (existing) {
      throw new InterlockError(409, 'EXAM_ALREADY_ACTIVE',
        `本副本已有一场进行中的考核（#${existing.id}，学员：${existing.student}），` +
        `同一副本只允许一场进行中的考核；请待其交卷或由检查点回退/站场重置终止后再开考`,
        { examId: existing.id });
    }

    // 未知进路：全部目标都必须是当前站场存在的进路定义
    const missing = ids.filter((r) => !db.prepare(`SELECT 1 FROM routes WHERE id=?`).get(r));
    if (missing.length) {
      throw new InterlockError(404, 'EXAM_ROUTE_NOT_FOUND',
        `不能开考：进路 ${[...new Set(missing)].join('、')} 在当前站场不存在（未知进路）`);
    }

    // 起始状态水位：开考时已存在的最大进路实例编号（-1 表示无实例）
    const maxRow = db.prepare(`SELECT COALESCE(MAX(id), -1) AS m FROM route_instances`).get();
    const watermark = Number(maxRow.m);

    let info;
    try {
      info = db.prepare(
        `INSERT INTO exams(student,status,started_by,started_at,start_instance_watermark,
                           active_elapsed_ms,run_started_at)
         VALUES(?, 'active', ?, ?, ?, 0, ?)`
      ).run(studentName, operator || '管理员', now, watermark, now);
    } catch (e) {
      if (String(e.code) === 'SQLITE_CONSTRAINT_UNIQUE') {
        const cur = activeExamRow();
        throw new InterlockError(409, 'EXAM_ALREADY_ACTIVE',
          `本副本已有一场进行中的考核（#${cur ? cur.id : '?'}，学员：${cur ? cur.student : '?'}），同一副本只允许一场进行中的考核`,
          cur ? { examId: cur.id } : {});
      }
      throw e;
    }
    const id = info.lastInsertRowid;

    const insGoal = db.prepare(
      `INSERT INTO exam_targets(exam_id,goal_order,route_id,status) VALUES(?,?,?, 'pending')`
    );
    ids.forEach((routeId, idx) => insGoal.run(id, idx + 1, routeId));
    return id;
  })();

  return getExam(examId);
}

/* --------------------- 联锁钩子：开放（判定占目标/偏差） --------------------- */

/**
 * 进路实例成功开放后（直接开放或等待自动兑现），在【同一事务】内记录考核。
 *  - 仅当存在进行中考核、实例属于该学员、且为开考后新实例时处理；
 *  - 进路等于当前待达成目标且该目标尚未占住实例：把实例写为该目标的占住实例；
 *  - 否则记为顺序外偏差（同一实例只记一次）。
 * 幂等：重复触发同一实例不会重复记账（目标已占住 / 偏差已存在）。
 */
function onInstanceOpenedInTxn(instanceId) {
  const db = getDb();
  const exam = activeExamRow();
  if (!exam) return;

  const inst = db.prepare(`SELECT * FROM route_instances WHERE id=?`).get(instanceId);
  if (!inst) return;
  // 只统计归属该学员的开考后新实例
  if (inst.opened_by !== exam.student) return;
  if (Number(instanceId) <= Number(exam.start_instance_watermark)) return;

  // 已占住某目标（幂等重入）：不重复处理
  const held = db.prepare(`SELECT id FROM exam_targets WHERE exam_id=? AND instance_id=?`)
    .get(exam.id, instanceId);
  if (held) return;
  // 已记偏差（幂等重入）
  const dev = db.prepare(`SELECT id FROM exam_deviations WHERE instance_id=?`).get(instanceId);
  if (dev) return;

  const current = db.prepare(
    `SELECT * FROM exam_targets WHERE exam_id=? AND status='pending' ORDER BY goal_order LIMIT 1`
  ).get(exam.id);

  if (current && current.route_id === inst.route_id && current.instance_id == null) {
    // 与当前待达成目标相符：占住该目标（尚未完成，需逐段走行正常解除）
    try {
      db.prepare(`UPDATE exam_targets SET instance_id=? WHERE id=? AND instance_id IS NULL`)
        .run(instanceId, current.id);
    } catch (e) {
      if (String(e.code) === 'SQLITE_CONSTRAINT_UNIQUE') {
        // 并发/重复路径已占住：忽略
      } else throw e;
    }
    return;
  }

  // 顺序外成功开放：记偏差（之后完成也不补算）
  try {
    db.prepare(
      `INSERT INTO exam_deviations(exam_id,instance_id,route_id,goal_order,reason,created_at)
       VALUES(?,?,?,?,?,?)`
    ).run(
      exam.id, instanceId, inst.route_id, current ? current.goal_order : null,
      current
        ? `学员在顺序外成功开放进路 ${inst.route_id}（当前待达成目标为第 ${current.goal_order} 项目标 ${current.route_id}），记为偏差；该实例之后完成也不补算`
        : `学员在无待达成目标时成功开放进路 ${inst.route_id}，记为偏差`,
      Date.now()
    );
  } catch (e) {
    if (String(e.code) !== 'SQLITE_CONSTRAINT_UNIQUE') throw e;
  }
}

/* ----------------- 联锁钩子：逐段走行正常解除（推进目标） ----------------- */

/**
 * 进路实例在逐段走行后【正常解除】（末段清空自动解除）时，于同一事务内调用。
 * 若该实例正占住某考核目标：标记 completed、记录完成时刻（推进）。
 * 幂等：目标已 completed 不重复处理。人工释放/延时取消走另一个钩子，不会调用本函数。
 */
function onInstanceNormalReleasedInTxn(instanceId) {
  const db = getDb();
  const target = db.prepare(
    `SELECT * FROM exam_targets WHERE instance_id=? AND status='pending'`
  ).get(instanceId);
  if (!target) return;
  db.prepare(
    `UPDATE exam_targets SET status='completed', completed_at=? WHERE id=? AND status='pending'`
  ).run(Date.now(), target.id);
}

/**
 * 进路实例被【非正常走行】方式解除（人工提前/兜底解除、延时取消保护期核验解除）时，
 * 于同一事务内调用：若它占住着某目标，释放占用（目标回到未占住，可重新排路完成）。
 * 已完成的目标不动；偏差实例本来就未占目标。不算完成、不推进。
 */
function onInstanceAbnormalReleasedInTxn(instanceId) {
  const db = getDb();
  db.prepare(
    `UPDATE exam_targets SET instance_id=NULL WHERE instance_id=? AND status='pending'`
  ).run(instanceId);
}

/* ------------------------------ 交卷 ------------------------------ */

/**
 * 学员交卷（只能由被指定学员本人）。冻结有效用时，返回逐项目标状态、偏差与用时。
 * 已交卷后重复交卷幂等返回原成绩；已终止考核交卷拒绝。
 */
function submitExam(examId, student) {
  const db = getDb();
  const id = Number(examId);
  if (!Number.isInteger(id)) {
    throw new InterlockError(400, 'EXAM_ID_INVALID', `考核编号必须是整数（收到 ${examId}）`);
  }

  let row = getExamRow(id);
  if (!row) {
    throw new InterlockError(404, 'EXAM_NOT_FOUND', `考核 ${id} 不存在（或不属于当前副本）`);
  }
  if (row.status === 'terminated') {
    throw new InterlockError(409, 'EXAM_TERMINATED',
      `考核 #${id} 已终止（${row.terminated_reason || '检查点回退或站场重置'}），不能交卷；已终止考核不可再交卷`);
  }
  if (row.student !== student) {
    throw new InterlockError(403, 'EXAM_NOT_ASSIGNED_STUDENT',
      `不能交卷：考核 #${id} 指定的学员为 ${row.student}，当前值班员 ${student} 不是该考核学员`);
  }

  if (row.status === 'active') {
    db.transaction(() => {
      const cur = getExamRow(id);
      if (!cur || cur.status !== 'active') return; // 并发已收尾
      const elapsed = elapsedMsOf(cur);
      db.prepare(
        `UPDATE exams SET status='submitted', submitted_at=?, ended_at=?,
                final_elapsed_ms=?, active_elapsed_ms=?, run_started_at=NULL WHERE id=?`
      ).run(Date.now(), Date.now(), elapsed, elapsed, id);
    })();
    row = getExamRow(id);
  }
  // status === 'submitted'：重复交卷幂等返回原成绩
  return getExam(row.id);
}

/* -------------- 检查点回退 / 站场重置：终止进行中考核 -------------- */

/**
 * 终止当前副本进行中（active）的考核（在调用方同一事务内调用）。
 * 已交卷（submitted）成绩保留、不动；无进行中考核时返回 null。
 * @returns {number|null} 被终止的考核编号
 */
function terminateActiveBySystemInTxn(reasonText) {
  const db = getDb();
  const exam = activeExamRow();
  if (!exam) return null;
  const now = Date.now();
  const elapsed = elapsedMsOf(exam, now);
  db.prepare(
    `UPDATE exams SET status='terminated', terminated_reason=?, ended_at=?,
            final_elapsed_ms=?, active_elapsed_ms=?, run_started_at=NULL WHERE id=?`
  ).run(reasonText || '检查点回退或站场重置，考核终止', now, elapsed, elapsed, exam.id);
  return exam.id;
}

/* -------------- 暂停/恢复：冻结/继续有效用时（跨库事务） -------------- */

/**
 * 暂停当前副本：冻结其进行中考核的有效用时（把当前运行段并入累计、清空起点）。
 * 在暂停的主库跨库事务【内】调用（db 已 ATTACH 业务库为 alias）。
 */
function freezeOnPauseInAttachedTxn(db, alias, now) {
  return db.prepare(
    `UPDATE ${alias}.exams SET active_elapsed_ms = active_elapsed_ms + (? - run_started_at),
            run_started_at = NULL
      WHERE status='active' AND run_started_at IS NOT NULL`
  ).run(now).changes;
}

/**
 * 恢复当前副本：继续进行中考核的有效用时（重设运行段起点）；被冻结的考核
 * （run_started_at 为 NULL，即暂停时处于 active）重新计时。
 * 在恢复的主库跨库事务【内】调用（db 已 ATTACH 业务库为 alias）。
 */
function resumeOnResumeInAttachedTxn(db, alias, now) {
  return db.prepare(
    `UPDATE ${alias}.exams SET run_started_at=?
      WHERE status='active' AND run_started_at IS NULL`
  ).run(now).changes;
}

/* ------------------------------ 查询 / 序列化 ------------------------------ */

function rowToTarget(row) {
  return {
    goalOrder: row.goal_order,
    routeId: row.route_id,
    status: row.status,                 // pending / completed
    instanceId: row.instance_id,
    completedAt: row.completed_at
  };
}

function rowToDeviation(row) {
  return {
    id: row.id,
    instanceId: row.instance_id,
    routeId: row.route_id,
    goalOrder: row.goal_order,
    reason: row.reason,
    createdAt: row.created_at
  };
}

function rowToExam(row, now = Date.now()) {
  if (!row) return null;
  const db = getDb();
  const targets = db.prepare(
    `SELECT * FROM exam_targets WHERE exam_id=? ORDER BY goal_order`
  ).all(row.id).map(rowToTarget);
  const deviations = db.prepare(
    `SELECT * FROM exam_deviations WHERE exam_id=? ORDER BY id`
  ).all(row.id).map(rowToDeviation);
  const completed = targets.filter((t) => t.status === 'completed').length;
  const nextTarget = targets.find((t) => t.status === 'pending') || null;

  return {
    id: row.id,
    student: row.student,
    status: row.status,                 // active / submitted / terminated
    startedBy: row.started_by,
    startedAt: row.started_at,
    startInstanceWatermark: row.start_instance_watermark,
    submittedAt: row.submitted_at,
    terminatedReason: row.terminated_reason,
    endedAt: row.ended_at,
    targets,
    targetCount: targets.length,
    completedCount: completed,
    nextGoalOrder: nextTarget ? nextTarget.goalOrder : null,
    nextRouteId: nextTarget ? nextTarget.routeId : null,
    deviations,
    deviationCount: deviations.length,
    // 有效用时（毫秒；active 随时间增长、暂停冻结；终态为固化值）与秒/文本
    elapsedMs: elapsedMsOf(row, now),
    createdAt: row.created_at
  };
}

function getExam(id) {
  return rowToExam(getExamRow(Number(id)));
}

/**
 * 考核列表：进行中在前（编号倒序），交卷/终止在后（结束时刻倒序）。
 * @param {{student?:string, status?:string}} filter
 */
function listExams(filter = {}) {
  const db = getDb();
  let rows = db.prepare(
    `SELECT * FROM exams
      ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END,
               CASE status WHEN 'active' THEN id ELSE -COALESCE(ended_at, id) END DESC`
  ).all();
  if (filter.student) rows = rows.filter((r) => r.student === filter.student);
  if (filter.status) rows = rows.filter((r) => r.status === filter.status);
  const now = Date.now();
  return rows.map((r) => rowToExam(r, now));
}

/** 进行中考核详情（无则 null；供 /api/state 快照下发，管理台与值班台显示进度） */
function getActiveExam() {
  const row = activeExamRow();
  return row ? rowToExam(row) : null;
}

module.exports = {
  startExam,
  submitExam,
  onInstanceOpenedInTxn,
  onInstanceNormalReleasedInTxn,
  onInstanceAbnormalReleasedInTxn,
  terminateActiveBySystemInTxn,
  freezeOnPauseInAttachedTxn,
  resumeOnResumeInAttachedTxn,
  getExam,
  listExams,
  getActiveExam
};
