import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df = df.drop_duplicates().reset_index(drop=True)
    df['index_id'] = df.index

    rest_dim = df[['rest_id','rest_name','cuisines','avg_cost','price_range']].reset_index(drop=True)
    rest_dim = rest_dim[['rest_id','rest_name','cuisines','avg_cost','price_range']]

    location_dim = df[['rest_id','city','address','locality','locality_verbose','longitude','latitude']].reset_index(drop=True)
    location_dim = location_dim[['rest_id','city','address','locality','locality_verbose','longitude','latitude']]

    booking_delivery_dim = df[['rest_id','table_booking','online_delivery','delivering_now','switch_to_order_menu']].reset_index(drop=True)
    booking_delivery_dim = booking_delivery_dim[['rest_id','table_booking','online_delivery','delivering_now','switch_to_order_menu']]

    rating_dim = df[['rest_id','agg_rating','rating_color','rating_text','votes']].reset_index(drop=True)
    rating_dim = rating_dim[['rest_id','agg_rating','rating_color','rating_text','votes']]

    fact_table = df.merge(rest_dim, on='rest_id') \
             .merge(location_dim, on='rest_id') \
             .merge(booking_delivery_dim, on='rest_id') \
             .merge(rating_dim, on='rest_id') \
             [['index_id','country_code','country', 'rest_id']]

    return {"rest_dim":rest_dim.to_dict(orient="dict"),
    "location_dim":location_dim.to_dict(orient="dict"),
    "booking_delivery_dim":booking_delivery_dim.to_dict(orient="dict"),
    "rating_dim":rating_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
