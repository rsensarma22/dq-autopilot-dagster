# from dagster import Definitions
# from dq_autopilot.resources.postgres import postgres
# from dq_autopilot.assets.dq_assets import (
#     discovered_tables,
#     table_profiles,
#     write_dq_results,
#     dq_failures,
#     dq_alerts,
# )

# defs = Definitions(
#     assets=[discovered_tables, table_profiles, write_dq_results, dq_failures, dq_alerts],
#     resources={"postgres": postgres},
# )

#Automated script for 9am runs

from dagster import Definitions, ScheduleDefinition, define_asset_job
from dq_autopilot.resources.postgres import postgres
from dq_autopilot.assets.dq_assets import (
    discovered_tables,
    table_profiles,
    write_dq_results,
    dq_failures,
    dq_alerts,
)

dq_job = define_asset_job(name="dq_autopilot_job")

daily_9am = ScheduleDefinition(
    name="daily_dq_autopilot_9am",
    cron_schedule="0 9 * * *",
    job=dq_job,
)

defs = Definitions(
    assets=[discovered_tables, table_profiles, write_dq_results, dq_failures, dq_alerts],
    resources={"postgres": postgres},
    schedules=[daily_9am],
)