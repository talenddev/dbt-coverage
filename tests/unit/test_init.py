import pytest

from dbt_coverage import (
    Catalog,
    Column,
    CoverageDiff,
    CoverageReport,
    CoverageType,
    Table,
)


@pytest.mark.parametrize("cov_type", [CoverageType.DOC, CoverageType.TEST])
def test_coverage_report_with_zero_tables(cov_type):
    """Tests that CoverageReport handles a catalog with 0 tables without division by zero."""

    empty_catalog = Catalog(tables={})

    coverage_report = CoverageReport.from_catalog(empty_catalog, cov_type)

    assert len(coverage_report.covered) == 0
    assert coverage_report.hits == 0
    assert len(coverage_report.total) == 0
    assert coverage_report.coverage is None
    assert len(coverage_report.misses) == 0
    assert coverage_report.subentities == {}


@pytest.mark.parametrize("cov_type", [CoverageType.DOC, CoverageType.TEST])
def test_coverage_diff_with_zero_tables(cov_type):
    empty_catalog = Catalog(tables={})
    coverage_report_1 = CoverageReport.from_catalog(empty_catalog, cov_type)
    coverage_report_2 = CoverageReport.from_catalog(empty_catalog, cov_type)

    diff = CoverageDiff(coverage_report_1, coverage_report_2)

    assert len(diff.new_misses) == 0


def create_test_catalog():
    """Create a simple test catalog with some tables and columns."""
    # Create columns for table 1
    col1_1 = Column(name="id", doc=True, tests=2)
    col1_2 = Column(name="name", doc=True, tests=1)
    col1_3 = Column(name="email", doc=False, tests=0)

    # Create table 1
    table1 = Table(
        unique_id="model.test.users",
        name="users",
        original_file_path="models/users.sql",
        columns={"id": col1_1, "name": col1_2, "email": col1_3},
    )

    # Create columns for table 2
    col2_1 = Column(name="order_id", doc=True, tests=1)
    col2_2 = Column(name="user_id", doc=True, tests=1)
    col2_3 = Column(name="total", doc=True, tests=0)

    # Create table 2
    table2 = Table(
        unique_id="model.test.orders",
        name="orders",
        original_file_path="models/orders.sql",
        columns={"order_id": col2_1, "user_id": col2_2, "total": col2_3},
    )

    # Create catalog
    catalog = Catalog(
        tables={
            "model.test.users": table1,
            "model.test.orders": table2,
        }
    )

    return catalog


def test_doc_coverage_xml():
    """Test doc coverage XML output."""
    print("Testing DOC coverage XML output...")
    catalog = create_test_catalog()
    coverage_report = CoverageReport.from_catalog(catalog, CoverageType.DOC)

    xml_output = coverage_report.to_xml("/path/to/project")
    print("\n=== DOC Coverage XML Output ===")
    print(xml_output)
    print("\n" + "=" * 50)

    # Basic validation
    assert '<?xml version="1.0" ?>' in xml_output
    assert "coverage" in xml_output
    assert "line-rate" in xml_output
    assert "users" in xml_output
    assert "orders" in xml_output
    print("✓ DOC coverage XML output looks valid")


def test_test_coverage_xml():
    """Test test coverage XML output."""
    print("\nTesting TEST coverage XML output...")
    catalog = create_test_catalog()
    coverage_report = CoverageReport.from_catalog(catalog, CoverageType.TEST)

    xml_output = coverage_report.to_xml("/path/to/project")
    print("\n=== TEST Coverage XML Output ===")
    print(xml_output)
    print("\n" + "=" * 50)

    # Basic validation
    assert '<?xml version="1.0" ?>' in xml_output
    assert "coverage" in xml_output
    assert "hits" in xml_output
    print("✓ TEST coverage XML output looks valid")


def test_empty_catalog_xml():
    """Test XML output with empty catalog."""
    print("\nTesting empty catalog XML output...")
    empty_catalog = Catalog(tables={})
    coverage_report = CoverageReport.from_catalog(
        empty_catalog, CoverageType.DOC)

    xml_output = coverage_report.to_xml("/path/to/project")
    print("\n=== Empty Catalog XML Output ===")
    print(xml_output)
    print("\n" + "=" * 50)

    # Basic validation
    assert '<?xml version="1.0" ?>' in xml_output
    assert "coverage" in xml_output
    print("✓ Empty catalog XML output looks valid")
