"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pytest
import pandas as pd
from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Test that cancelled orders are excluded."""
    # Create sample data with one cancelled order
    orders = pd.DataFrame({
        'order_id': [1, 2],
        'customer_id': [1, 1],
        'status': ['completed', 'cancelled']
    })
    order_items = pd.DataFrame({
        'order_id': [1, 2],
        'product_id': [1, 1],
        'quantity': [2, 3]
    })
    products = pd.DataFrame({
        'product_id': [1],
        'unit_price': [10.0]
    })
    customers = pd.DataFrame({
        'customer_id': [1],
        'customer_name': ['Test Customer']
    })

    data_dict = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }

    # Pass minimal config for tests (Tier 3 compatibility)
    config = {"pipeline_name": "test"}

    result = transform(data_dict, config)
    
    # Should only have the completed order
    assert len(result) == 1
    assert result['total_orders'].iloc[0] == 1


def test_transform_filters_suspicious_quantity():
    """Test that orders with quantity > 100 are excluded."""
    orders = pd.DataFrame({
        'order_id': [1, 2], 
        'customer_id': [1, 1], 
        'status': ['completed', 'completed']
    })
    order_items = pd.DataFrame({
        'order_id': [1, 2],
        'product_id': [1, 1],
        'quantity': [50, 150]   # one suspicious
    })
    products = pd.DataFrame({'product_id': [1], 'unit_price': [10.0]})
    customers = pd.DataFrame({'customer_id': [1], 'customer_name': ['Test']})

    data_dict = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }

    config = {"pipeline_name": "test"}

    result = transform(data_dict, config)
    
    # Should exclude the quantity 150 → only 1 order remains
    assert len(result) == 1
    assert result['total_orders'].iloc[0] == 1


def test_validate_catches_nulls():
    """Test that validate raises ValueError when customer_id is null."""
    df = pd.DataFrame({
        'customer_id': [None, 2],
        'customer_name': ['Test1', 'Test2'],
        'total_orders': [5, 3],
        'total_revenue': [100.0, 200.0],
        'avg_order_value': [20.0, 66.67],
        'top_category': ['Electronics', 'Clothing'],
        'is_outlier': [False, False],
        'z_score': [0.0, 0.0]
    })

    with pytest.raises(ValueError):
        validate(df)


# Optional: Test for empty DataFrame handling (good for Tier 3)
def test_transform_handles_empty_orders():
    """Test transform handles case with no new orders (incremental)."""
    data_dict = {
        "customers": pd.DataFrame(),
        "products": pd.DataFrame(),
        "orders": pd.DataFrame(),
        "order_items": pd.DataFrame()
    }
    config = {"pipeline_name": "test"}
    
    result = transform(data_dict, config)
    
    assert len(result) == 0
    assert list(result.columns) == ['customer_id', 'customer_name', 'total_orders', 
                                   'total_revenue', 'avg_order_value', 'top_category', 
                                   'is_outlier', 'z_score']
    