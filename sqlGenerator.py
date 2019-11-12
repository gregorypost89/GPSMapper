#query = open("locator.sql", "r")

#print(query.read())


def radius100(latDegInMi, longDegInMi):
    return("""
    SELECT city, state, latitude, longitude FROM zipcodes Z JOIN points P 
        ON P.GridX IN ( 
            SELECT GridX - 5, GridX - 4, GridX - 3, GridX - 2, GridX - 1, GridX, GridX + 1, GridX + 2, GridX + 3, GridX + 4, GridX + 5 
            FROM zipcodes ZX WHERE Z.id = ZX.id) 
        AND 
            P.GridY IN ( 
                SELECT GridY - 5, GridY - 4, GridY - 3, GridY - 2, GridY - 1, GridY, GridY + 1, GridY + 2, GridY + 3, GridY + 4, GridY + 5 
            FROM zipcodes ZY WHERE Z.id = ZY.id) 
    WHERE P.Status = A 
        AND((Z.latitude - P.latitude) * """ + str(latDegInMi) + " ^ 2 + (Z.longitude - P.longitude) *"
        + str(longDegInMi) + """" ^ 2 < (100^2) 
    GROUP BY city, state, latitude, longitude;""")

