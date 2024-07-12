from pydantic import BaseModel, Field, field_validator,ValidationError
from typing import Optional
from datetime import datetime

class LogisticData(BaseModel):
    GpsProvider: str
    BookingID: str
    Market_Regular: str
    BookingID_Date: datetime
    vehicle_no: str
    Origin_Location: str
    Destination_Location: str
    Org_lat_lon: str
    Des_lat_lon: str
    Data_Ping_time: str
    Planned_Eta: str
    Current_Location: str
    DestinationLocation: str
    actual_eta: Optional[datetime] = None
    Curr_lat: Optional[float] = None
    Curr_lon: Optional[float] = None
    ontime: Optional[str] = None
    delay: Optional[str] = None
    OriginLocation_Code: str
    DestinationLocation_Code: str
    trip_start_date: datetime
    trip_end_date: Optional[datetime] = None
    TRANSPORTATION_DISTANCE_IN_KM: Optional[int] = None
    vehicle_type: Optional[str] = None
    Minimum_kms_to_be_covered_in_a_day: Optional[int] = None
    Driver_Name: Optional[str] = None
    Driver_MobileNo: Optional[str] = None
    customerId: Optional[str] = None
    customerNameCode: Optional[str] = None
    supplierId: Optional[str] = None
    supplierNameCode: Optional[str] = None
    Material_Shipped: Optional[str] = None

    @field_validator('BookingID_Date', 'trip_start_date', 'trip_end_date', 'actual_eta', mode='before')
    def parse_dates(cls, value):
        if isinstance(value, str):
            return datetime.strptime(value, '%m/%d/%Y %H:%M')
        return value

    @field_validator('Curr_lat', 'Curr_lon', 'TRANSPORTATION_DISTANCE_IN_KM', 'Minimum_kms_to_be_covered_in_a_day', mode='before')
    def parse_floats(cls, value):
        if isinstance(value, str) and value:
            return float(value)
        return value

    @field_validator('Data_Ping_time', 'Planned_Eta', mode='before')
    def validate_time_format(cls, value):
        try:
            datetime.strptime(value, '%H:%M.%S')
            return value
        except ValueError:
            raise ValueError(f"Invalid time format: {value}")



# --------- testing 

def datatype_validation_process(msg):
    try:
        logistic_data = LogisticData(**msg)
        print(f"validated message: {logistic_data.model_dump(by_alias=True)}")
    except ValidationError as e:
        print(f"Validation error:{e}")


value = {'GpsProvider': 'CONSENT TRACK', 'BookingID': 'MVCV0000927/082021', 'Market_Regular': None, 'BookingID_Date': '8/17/2020', 'vehicle_no': 'KA590408', 'Origin_Location': 'TVSLSL-PUZHAL-HUB,CHENNAI,TAMIL NADU', 'Destination_Location': 'ASHOK LEYLAND PLANT 1- HOSUR,HOSUR,KARNATAKA', 'Org_lat_lon': '13.1550,80.1960', 'Des_lat_lon': '12.7400,77.8200', 'Data_Ping_time': '05:09.0', 'Planned_ETA': '59:01.0', 'Current_Location': 'Vaniyambadi Rd, Valayambattu, Tamil Nadu 635752, India', 'DestinationLocation': 'ASHOK LEYLAND PLANT 1- HOSUR,HOSUR,KARNATAKA', 'actual_eta': '8/28/2020 14:38', 'Curr_lat': '12.6635', 'Curr_lon': '78.64987', 'ontime': 'nan', 'delay': 'R', 'OriginLocation_Code': 'CHEPUZTVSHUA1', 'DestinationLocation_Code': 'HOSHOSALLCCA2', 'trip_start_date': '8/17/2020 14:59', 'trip_end_date': 'nan', 'TRANSPORTATION_DISTANCE_IN_KM': '320.0', 'vehicleType': 'nan', 'Minimum_kms_to_be_covered_in_a_day': 'nan', 'Driver_Name': 'nan', 'Driver_MobileNo': 'nan', 'customerID': 'ALLEXCHE45', 'customerNameCode': 'Ashok leyland limited', 'supplierID': 'VIJEXHOSR7', 'supplierNameCode': 'VIJAY TRANSPORT', 'Material_Shipped': None}

datatype_validation_process(value)