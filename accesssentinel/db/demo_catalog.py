"""Demonstration catalog: three protected applications sharing one user identity, resources with
sensitivity labels, roles and a role-based first-line ACL (assumption A7 in docs/PHASE1_AUDIT.md).

This is test data for the research prototype, not a real organisation's policy.
"""
from typing import Dict, List, Tuple

# app id -> (display name, internal base URL, description)
APPS: Dict[str, Tuple[str, str, str]] = {
    "hr": ("HR Portal", "http://app-hr:8000", "Employee records, payroll and performance reviews"),
    "finance": ("Finance Portal", "http://app-finance:8000", "Invoices, payments, budgets and audit reports"),
    "docs": ("Document Store", "http://app-docs:8000", "Handbook, policies, contracts and board minutes"),
}

# app id -> [(resource key, sensitivity s, description)]
RESOURCES: Dict[str, List[Tuple[str, int, str]]] = {
    "hr": [("directory", 0, "Staff directory"),
           ("employees", 0, "Employee profiles"),
           ("payroll", 1, "Salaries and bank details"),
           ("reviews", 1, "Performance reviews"),
           ("recruiting", 0, "Open positions")],
    "finance": [("invoices", 0, "Customer invoices"),
                ("budget", 0, "Departmental budgets"),
                ("payments", 1, "Outgoing payments"),
                ("vendor-banking", 1, "Vendor bank account details"),
                ("audit-reports", 1, "Internal audit reports")],
    "docs": [("handbook", 0, "Employee handbook"),
             ("policies", 0, "Company policies"),
             ("contracts", 1, "Customer and supplier contracts"),
             ("board-minutes", 1, "Board meeting minutes"),
             ("ip-filings", 1, "Patent and IP filings")],
}

# Every role gets these grants.
_BASE = {("hr", "directory"): "read", ("docs", "handbook"): "read", ("docs", "policies"): "read"}

# role -> {(app, resource): "comma-separated permissions"}
_ROLE_GRANTS: Dict[str, Dict[Tuple[str, str], str]] = {
    "staff": {},
    "hr_manager": {("hr", "employees"): "read,write,admin", ("hr", "payroll"): "read,write,export",
                   ("hr", "reviews"): "read,write", ("hr", "recruiting"): "read,write,admin"},
    "finance_analyst": {("finance", "invoices"): "read,write,export", ("finance", "budget"): "read,write",
                        ("finance", "payments"): "read,write", ("finance", "vendor-banking"): "read",
                        ("finance", "audit-reports"): "read"},
    "legal": {("docs", "contracts"): "read,write,export", ("docs", "ip-filings"): "read,write",
              ("hr", "employees"): "read"},
    "executive": {("docs", "board-minutes"): "read,write", ("finance", "budget"): "read",
                  ("finance", "audit-reports"): "read", ("hr", "employees"): "read", ("hr", "reviews"): "read"},
    "auditor": {("finance", "invoices"): "read,export", ("finance", "payments"): "read",
                ("finance", "audit-reports"): "read,export", ("hr", "payroll"): "read"},
    "it_admin": {("docs", "policies"): "read,write,admin", ("docs", "handbook"): "read,write",
                 ("hr", "directory"): "read,write"},
}

ROLES = sorted(_ROLE_GRANTS)

# (user id, display name, role, department)
USERS: List[Tuple[str, str, str, str]] = [
    ("alice", "Alice Chen", "hr_manager", "Human Resources"),
    ("bob", "Bob Kumar", "finance_analyst", "Finance"),
    ("carol", "Carol Diaz", "staff", "Engineering"),
    ("dave", "Dave Okafor", "staff", "Sales"),
    ("erin", "Erin Novak", "legal", "Legal"),
    ("frank", "Frank Muller", "executive", "Executive Office"),
    ("grace", "Grace Lee", "auditor", "Internal Audit"),
    ("henry", "Henry Silva", "it_admin", "IT"),
    ("ivan", "Ivan Petrov", "staff", "Engineering"),
    ("judy", "Judy Tanaka", "finance_analyst", "Finance"),
]


# Security staff for the analyst dashboard (Phase 9). Their roles have no application grants (default deny).
SECURITY_USERS: List[Tuple[str, str, str, str]] = [
    ("analyst1", "Analyst One", "security_analyst", "Security Operations"),
    ("secadmin", "Security Admin", "security_admin", "Security Operations"),
]
SECURITY_ROLES = ("security_analyst", "security_admin")


def acl_rows() -> List[Tuple[str, str, str, str]]:
    """[(app, resource, role, permission)], deduplicated and sorted."""
    known = {(a, r) for a, rs in RESOURCES.items() for r, _, _ in rs}
    rows = set()
    for role, grants in _ROLE_GRANTS.items():
        merged = dict(_BASE)
        for k, v in grants.items():
            merged[k] = ",".join(sorted(set(merged.get(k, "").split(",") + v.split(",")) - {""}))
        for (a, r), perms in merged.items():
            if (a, r) not in known:
                raise ValueError(f"ACL references unknown resource {a}:{r}")
            for p in perms.split(","):
                rows.add((a, r, role, p))
    return sorted(rows)
