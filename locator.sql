SELECT city, state, latitude, longitude, COUNT(*)
FROM zipcodes Z
JOIN points P
	ON P.GridX IN (
      SELECT GridX - 5, GridX - 4, GridX - 3, GridX - 2, GridX - 1, GridX, GridX + 1, GridX + 2, GridX + 3, GridX + 4, GridX + 5
      FROM zipcode ZX WHERE Z.id = ZX.id)
     AND
      P.GridY IN (
      SELECT GridY - 5, GridY - 4, GridY - 3, GridY - 2, GridY - 1, GridY, GridY + 1, GridY + 2, GridY + 3, GridY + 4, GridY + 5
      FROM zipcode ZY WHERE Z.id = ZY.id)
WHERE P.Status = A
 AND((Z.latitude - P.latitude) * LatDegInMi ^ 2
  + ((Z.longitude - P.longitude) * LongDegInMi ^ 2 < (100^2)
GROUP BY city, state, latitude, longitude;