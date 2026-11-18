ROLES=('admin','operator','viewer')
STATUSES=('queued','running','passed','failed')
OPERATIONS=('put','delete')
# 在线迁移：待搬迁 → 复制中 → 已切换；可中止/失败后恢复复制中。
MIGRATION_STATUSES=('pending','copying','switched')
MIGRATION_PHASES=('seed','before_switch','after_switch')
