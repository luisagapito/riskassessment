WITH AnomalyDetectionStep AS
(
    SELECT
        placa,
        date,
        cast(time as datetime) AS time,
        CAST(acceleration AS float) AS acceleration,
        AnomalyDetection_SpikeAndDip(CAST(acceleration AS float), 95, 120, 'spikesanddips')
            OVER(LIMIT DURATION(second, 120)) AS SpikeAndDipScores
    FROM  [riskstreamhub]
), Anomalies AS(
SELECT
    placa,
    date,
    time,
    acceleration,
    CAST(GetRecordPropertyValue(SpikeAndDipScores, 'IsAnomaly') AS bigint) AS IsAnomaly
FROM  AnomalyDetectionStep
where CAST(GetRecordPropertyValue(SpikeAndDipScores, 'IsAnomaly') AS bigint)  =  1
)

SELECT
CONCAT(car_data.placa,risk.place,car_data.date) as validacion,
  car_data.placa,
  risk.place,
  risk.latitude,
  risk.longitude,
  car_data.date,
  cast(car_data.time as datetime) as time,
  1 AS status_risk
INTO
  [locationrisk]
FROM
  [riskstreamhub] AS car_data
JOIN
 [riskdlkstorage] as risk
ON
  cast(risk.start_time as datetime)<cast(car_data.time as datetime)
  and cast(risk.end_time as datetime)>cast(car_data.time as datetime)
  and ST_DISTANCE(CreatePoint(car_data.latitude,car_data.longitude), CreatePoint(risk.latitude,risk.longitude)) < 100



SELECT 
placa,
date,
time,
acceleration,
IsAnomaly
INTO [accelerationrisk]
FROM Anomalies