# ─────────────────────────────────────────────────────────────────
# api/schema.py
#
# Defines the GraphQL schema for mini-ATLAS API
# Types:        ProjectPerformance
#               EmployeeUtilisation
#               RevenueSummary
# Queries:      projectPerformance
#               employeeUtilisation
#               revenueSummary
# Library:      Strawberry (Python GraphQL)
# ─────────────────────────────────────────────────────────────────

import strawberry
from typing import List, Optional
from api.main import query_to_json

# ── TYPES ─────────────────────────────────────────────────────────
# Each type defines the shape of one Gold mart table
# Field names must match column names from the SQL query

@strawberry.type
class ProjectPerformance:
    project_id:        str
    project_name:      str
    client:            str
    project_type:      str
    status:            str
    budget_usd:        Optional[float]
    total_hours:       Optional[float]
    avg_billing_rate:  Optional[float]
    total_labour_cost: Optional[float]
    total_revenue:     Optional[float]
    profit:            Optional[float]
    profit_margin_pct: Optional[float]

@strawberry.type
class EmployeeUtilisation:
    employee_id:             str
    first_name:              str
    last_name:               str
    role:                    str
    department:              str
    total_hours:             Optional[float]
    billable_hours:          Optional[float]
    non_billable_hours:      Optional[float]
    utilisation_pct:         Optional[float]
    total_revenue_generated: Optional[float]

@strawberry.type
class RevenueSummary:
    project_id:       str
    project_name:     str
    project_client:   str
    total_invoices:   Optional[int]
    total_paid:       Optional[float]
    total_unpaid:     Optional[float]
    total_overdue:    Optional[float]
    total_invoiced:   Optional[float]
    avg_days_overdue: Optional[float]

# ── QUERIES ───────────────────────────────────────────────────────
@strawberry.type
class Query:

    @strawberry.field
    def project_performance(self) -> List[ProjectPerformance]:
        """Returns profitability metrics for all projects."""
        sql = """
            SELECT
                project_id, project_name, client,
                project_type, status, budget_usd,
                total_hours, avg_billing_rate,
                total_labour_cost, total_revenue,
                profit, profit_margin_pct
            FROM mart_project_perf
            ORDER BY profit DESC
        """
        rows = query_to_json(sql)
        return [ProjectPerformance(**row) for row in rows]

    @strawberry.field
    def employee_utilisation(self) -> List[EmployeeUtilisation]:
        """Returns utilisation metrics for all employees."""
        sql = """
            SELECT
                employee_id, first_name, last_name,
                role, department, total_hours,
                billable_hours, non_billable_hours,
                utilisation_pct, total_revenue_generated
            FROM mart_utilisation
            ORDER BY utilisation_pct DESC
        """
        rows = query_to_json(sql)
        return [EmployeeUtilisation(**row) for row in rows]

    @strawberry.field
    def revenue_summary(self) -> List[RevenueSummary]:
        """Returns invoice and revenue summary per project."""
        sql = """
            SELECT
                project_id, project_name, project_client,
                total_invoices, total_paid, total_unpaid,
                total_overdue, total_invoiced,
                ISNULL(avg_days_overdue, 0) AS avg_days_overdue
            FROM mart_revenue_summary
            ORDER BY total_overdue DESC
        """
        rows = query_to_json(sql)
        return [RevenueSummary(**row) for row in rows]

# ── SCHEMA ────────────────────────────────────────────────────────
schema = strawberry.Schema(query=Query)