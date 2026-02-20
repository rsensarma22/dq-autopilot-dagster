from dagster import Definitions
from dq_autopilot.resources.postgres import postgres
from dq_autopilot.assets.dq_assets import (
    discovered_tables,
    table_profiles,
    write_dq_results,
    dq_alerts,
)

defs = Definitions(
    assets=[discovered_tables, table_profiles, write_dq_results, dq_alerts],
    resources={"postgres": postgres},
)