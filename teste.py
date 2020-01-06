import json


data = { "Professions": [
          {
            "Sector": "PRIVATE - 8112500",
            "Country": "BRAZIL",
            "CompanyIdNumber": "04759777000195",
            "CompanyName": "CONDOMINIO EDIFICIO AMERICAN BUSINESS CENTER",
            "Area": "UNKNOWN",
            "Level": "UNKNOWN",
            "Status": "INACTIVE",
            "IncomeRange": "SEM INFORMACAO",
            "Income": 0.0,
            "StartDate": "2013-10-01T00:00:00Z",
            "EndDate": "2015-10-03T00:00:00Z",
            "CreationDate": "2013-07-01T00:00:00Z",
            "LastUpdateDate": "2013-07-01T00:00:00Z"
          }]
       }

collections_to_insert2 = []
for record in data['Professions']:
    data_to_insert = []
    for k,v in record.items():
        data_to_insert.append(v)
    collections_to_insert2.append(tuple(data_to_insert))


print (collections_to_insert2)