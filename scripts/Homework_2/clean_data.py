if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def num_camel_case(columns):
    num = 0
    for s in columns:
        if (s != s.lower() and s != s.upper() and "_" not in s):
            num = num + 1


    return num

@transformer
def transform(data, *args, **kwargs):

    print(f"Preprocessing: rows with zero passengers: {data['passenger_count'].isin([0]).sum()}")
    data = data[data['passenger_count']>0]
    print(f"Preprocessing: rows with zero trip : {data['trip_distance'].isin([0]).sum()}")
    data = data[data['trip_distance']>0]


    print(f"Create a new column lpep_pickup_date by converting lpep_pickup_datetime")
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    print(f"The existing values of VendorID are {data['VendorID'].unique()}")
    print(f"Number of column in Camel Case:{num_camel_case(data.columns)}")

    print("Rename columns in Camel Case to Snake Case")
    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )
    
    

    return data


@test
def test_output(output, *args) -> None:

    assert ('vendor_id' in output.columns), 'vendor_id is not in the column'
    assert output['passenger_count'].isin([0]).sum() ==0, 'There are rides with zero passengers'

    assert output['trip_distance'].isin([0]).sum() ==0, 'There are rides with zero trip distance'
