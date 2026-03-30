create table if not exists Spatial_Object
(
    countryCode varchar (2),
    thematicIdIdentifier varchar,
    thematicIdIdentifierScheme varchar,
    monitoringSiteIdentifier varchar,
    monitoringSiteIdentifierScheme varchar,
    monitoringSiteName varchar,
    waterBodyIdentifier varchar,
    waterBodyIdentifierScheme varchar,
    waterBodyName varchar,
    specialisedZoneType varchar,
    naturalAWBHMWB varchar,
    reservoir varchar,
    surfaceWaterBodyTypeCode varchar,
    subUnitIdentifier varchar,
    subUnitIdentifierScheme varchar,
    subUnitName varchar,
    rbdIdentifier varchar,
    rbdIdentifierScheme varchar,
    rbdName varchar,
    confidentialityStatus varchar,
    lat real,
    lon real
);
create table if not exists EQR_Classification(
	countryCode varchar (2),
	parameterWaterBodyCategory varchar,
	parameterNCSWaterBodyType varchar,
	parameterNaturalAWBHMWB varchar,
	observedPropertyDeterminandBiologyEQRCode varchar,
	observedPropertyDeterminandLabel varchar,
	procedureClassificationSystem varchar,
	parameterBoundaryValueClasses12 real,
	parameterBoundaryValueClasses23 real,
	parameterBoundaryValueClasses34 real,
	parameterBoundaryValueClasses45 real,
	resultObservationStatus varchar,
	remarks varchar,
	metadata_versionId xml,
	metadata_beginLifeSpanVersion timestamp,
	metadata_statusCode varchar,
	metadata_observationStatus varchar,
	metadata_statements varchar,
	UID int
);
create table if not exists EQR_Data (
    countryCode varchar (2),
    monitoringSiteIdentifier varchar,
    monitoringSiteIdentifierScheme varchar,
    parameterWaterBodyCategory varchar,
    parameterNCSWaterBodyType varchar,
    parameterNaturalAWBHMWB varchar,
    observedPropertyDeterminandBiologyEQRCode varchar,
    observedPropertyDeterminandLabel varchar,
    procedureClassificationSystem varchar,
    phenomenonTimeReferenceYear int,
    parameterSamplingPeriod varchar,
    resultEcologicalStatusClassValue varchar,
    resultNumberOfSamples int,
    resultEQRValue real,
    resultNormalisedEQRValue real,
    resultObservationStatus varchar,
    remarks varchar,
    metadata_versionId varchar,
    metadata_beginLifeSpanVersion timestamp,
    metadata_statusCode varchar,
    metadata_observationStatus varchar,
    metadata_statements varchar,
    UID int
);
create table if not exists EQR_Data_By_WaterBody (
    countryCode varchar(2),
    waterBodyIdentifier varchar,
    waterBodyIdentifierScheme varchar,
    parameterWaterBodyCategory varchar,
    parameterNCSWaterBodyType varchar,
    parameterNaturalAWBHMWB varchar,
    observedPropertyDeterminandBiologyEQRCode varchar,
    observedPropertyDeterminandLabel varchar,
    procedureClassificationSystem varchar,
    phenomenonTimeReferenceYear int,
    parameterSamplingPeriod varchar,
    resultEcologicalStatusClassValue varchar,
    resultNumberOfSamples int,
    resultEQRValue real,
    resultNormalisedEQRValue real,
    resultObservationStatus varchar,
    remarks text,
    metadata_versionId varchar,
    metadata_beginLifeSpanVersion timestamp,
    metadata_statusCode varchar,
    metadata_observationStatus varchar,
    metadata_statements text,
    UID int
);

COPY Spatial_Object FROM 'C:\Program Files\PostgreSQL\16\data\Waterbase_v2022_1_S_WISE2_SpatialObject_DerivedData.csv' DELIMITER ',' CSV HEADER;
select * from Spatial_Object;

COPY EQR_Classification FROM 'C:\Program Files\PostgreSQL\16\data\Waterbase_v2022_1_T_WISE2_BiologyEQRClassificationProcedure.csv' DELIMITER ',' CSV HEADER;
select * from EQR_Classification;

COPY EQR_Data FROM 'C:\Program Files\PostgreSQL\16\data\Waterbase_v2022_1_T_WISE2_BiologyEQRData.csv' DELIMITER ',' CSV HEADER;
select * from EQR_Data;

COPY EQR_Data_By_WaterBody FROM 'C:\Program Files\PostgreSQL\16\data\Waterbase_v2022_1_T_WISE2_BiologyEQRDataByWaterBody.csv' DELIMITER ',' CSV HEADER;
select * from EQR_Data_By_WaterBody;


/*query 1: per latitudine maggiore di 48 gradi:
- media dello stato ecologico del corpo idrico, attraverso registrazioni
  ripetute più volte in diversi istanti di tempo per singoli siti
- percentuale della tipologia del corpo idrico ('Naturale')*/
select avg(EQR_Data.resultEcologicalStatusClassValue) as Media_Classe_Stato_Ecologico,
sum((EQR_Data.parameterNaturalAWBHMWB = 'Natural')::int) / 52538.0 * 100 as Percentuale_Tipologia_Corpo_Idrico
from Spatial_Object join EQR_Data on Spatial_Object.monitoringSiteIdentifier = EQR_Data.monitoringSiteIdentifier
where Spatial_Object.lat > 48
/*query 1.2 (2)*/
select
media_CLASSE_STATO_ECOLOGICO_nord48,
media_CLASSE_STATO_ECOLOGICO_sud48,
percentuale_Natural_nord48,
percentuale_Natural_sud48,
percentuale_HMWB_nord48,
percentuale_HMWB_sud48,
percentuale_AWB_nord48,
percentuale_AWB_sud48
from ( select
avg(UltimaAnalisiEQR.resultEcologicalStatusClassValue) as media_CLASSE_STATO_ECOLOGICO_nord48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'Natural')::int) * 100.0 / count(*) as percentuale_Natural_nord48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'HMWB')::int) * 100.0 / count(*) as percentuale_HMWB_nord48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'AWB')::int) * 100.0 / count(*) as percentuale_AWB_nord48
from Spatial_Object join ( select monitoringSiteIdentifier, resultEcologicalStatusClassValue, parameterNaturalAWBHMWB,
row_number() over (partition by monitoringSiteIdentifier order by phenomenonTimeReferenceYear desc)
as Ordine_Date from EQR_Data where phenomenonTimeReferenceYear is not NULL ) as UltimaAnalisiEQR on
Spatial_Object.monitoringSiteIdentifier = UltimaAnalisiEQR.monitoringSiteIdentifier and UltimaAnalisiEQR.Ordine_Date = 1
where Spatial_Object.lat > 48 and Spatial_Object.confidentialityStatus = 'F' ) as NORD48,
(
select
avg(UltimaAnalisiEQR.resultEcologicalStatusClassValue) as media_CLASSE_STATO_ECOLOGICO_sud48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'Natural')::int) * 100.0 / count(*) as percentuale_Natural_sud48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'HMWB')::int) * 100.0 / count(*) as percentuale_HMWB_sud48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'AWB')::int) * 100.0 / count(*) as percentuale_AWB_sud48
from Spatial_Object join ( select 
monitoringSiteIdentifier,
resultEcologicalStatusClassValue,
parameterNaturalAWBHMWB,
row_number() over (partition by monitoringSiteIdentifier order by phenomenonTimeReferenceYear desc) as Ordine_Date
from EQR_Data where phenomenonTimeReferenceYear is not NULL) as UltimaAnalisiEQR on
Spatial_Object.monitoringSiteIdentifier = UltimaAnalisiEQR.monitoringSiteIdentifier and UltimaAnalisiEQR.Ordine_Date = 1
where Spatial_Object.lat <= 48 and Spatial_Object.confidentialityStatus = 'F') as SUD48

/*query 3: perecentuale di observedPropertyDeterminandLabel per ogni stato in T2*/
select 
a.countryCode,
a.observedPropertyDeterminandLabel,
count(*) as count_per_label,
(count(*) * 100.0 / CountryTotals.count_total) as percentuale
from eqr_classification a join
( select countryCode, count(*) as count_total
from eqr_classification
group by
countryCode
) as CountryTotals on a.countryCode = CountryTotals.countryCode
group by
a.countryCode,
a.observedPropertyDeterminandLabel,
CountryTotals.count_total
having (count(*) * 100.0 / CountryTotals.count_total) > 5.0
order by
a.countryCode, count_per_label desc



/*query 4: quali sono i corpi idrici con più siti di monitoraggio per ogni stato (magari top 5)*/
select
countryCode,
waterbodyName,
subUnitName,
centri_di_ricerca_per_corpo
from ( select 
countryCode,
waterbodyName,
subUnitName,
centri_di_ricerca_per_corpo,
row_number() over (partition by countryCode order by centri_di_ricerca_per_corpo desc) as classifica
from ( select 
countryCode,
waterbodyName,
subUnitName,
count(*) as centri_di_ricerca_per_corpo
from spatial_object
group by 
countryCode, waterbodyName, subUnitName) as CorpiIdrici
) as Classifica_CorpiIdrici
where classifica <= 5
order by 
countryCode, centri_di_ricerca_per_corpo desc

/*query 5 (RIVEDERE): lista siti di monitoraggio europei affiancata alla lista degli schemi di monitoraggio*/
select 
    count(*) as num_eu_monitoring_sites
from 
    Spatial_Object
where 
    Spatial_Object.lat >= 55 and Spatial_Object.lat < 71
    and Spatial_Object.monitoringSiteIdentifierScheme = 'euMonitoringSiteCode';

select 
Spatial_Object.monitoringSiteIdentifier as Siti_di_monitoraggio,
Spatial_Object.monitoringSiteIdentifierScheme as Schemi_di_monitoraggio
from Spatial_Object
where Spatial_Object.lat >= 55 and Spatial_Object.lat < 71 and Spatial_Object.monitoringSiteIdentifierScheme is not NULL
and Spatial_Object.monitoringSiteIdentifier is not null and Spatial_Object.monitoringSiteIdentifierScheme = 'euMonitoringSiteCode';

/*query 6: numero di corpi idrici che sono 'reservoir'*/
select
rbdidentifier,
rbdname,
count(case when reservoir like 'Reservoir%' then 1 else NULL end) as reservoir_count
from spatial_object
group by rbdidentifier, rbdname
order by reservoir_count desc;

/*query 7: lista stati con più laghi*/
select Spatial_Object.countryCode, count (*) as n_laghi
from Spatial_Object
where Spatial_Object.specialisedZoneType = 'lakeWaterBody'
group by Spatial_Object.countryCode
order by n_laghi desc
limit 3

/*query 8: usiamo l'id dei siti di monitoraggio per ricavare qualcosa dalla terza ad esempio gli eqr norm
che attraverso una specifica tabella sono associati ad una classe di stato ecologico*/
select distinct Spatial_Object.monitoringSiteIdentifier, EQR_Data.resultNormalisedEQRValue,
EQR_Data.resultEcologicalStatusClassValue as Classe_stato_ecologico_associata
from Spatial_Object right join EQR_Data on Spatial_Object.monitoringSiteIdentifier = EQR_Data.monitoringSiteIdentifier
where EQR_Data.parameterNaturalAWBHMWB='Natural'

/*query 8.1 (9)*/
select 
waterbodyname,
avg(eqr_data.resultEcologicalStatusClassValue) as avg_eco_status,
max(eqr_data.phenomenonTimeReferenceYear) as ultimo_campionamento 
from spatial_object join eqr_data on spatial_object.monitoringSiteIdentifier = eqr_data.monitoringSiteIdentifier
where eqr_data.resultNumberOfSamples > 5
group by waterbodyname
having avg(eqr_data.resultEcologicalStatusClassValue) < 2
order by avg_eco_status, ultimo_campionamento desc;

/*query 10: quali tipi di corpi idrici sono associati alle varie categori di natural/AWB/HMWB.
(Tipologie corpi idrici: riverWaterBody, lakeWaterBody, coastalWaterBody)*/
select count (*) as numero_fiumi_naturali
from Spatial_Object
where Spatial_Object.specialisedZoneType = 'riverWaterBody' and Spatial_Object.naturalAWBHMWB = 'Natural'
group by Spatial_Object.specialisedZoneType, Spatial_Object.naturalAWBHMWB

/*query 11*/
select Spatial_Object.monitoringSiteName,  Spatial_Object.surfaceWaterBodyTypeCode, EQR_Data.phenomenonTimeReferenceYear
from Spatial_Object  
left join EQR_Data on Spatial_Object.monitoringsiteidentifier = EQR_Data.monitoringsiteidentifier

1)
select avg(EQR_Data.resultEcologicalStatusClassValue) as Media_Classe_Stato_Ecologico,
sum((EQR_Data.parameterNaturalAWBHMWB = 'Natural')::int) / 52538.0 * 100 as Percentuale_Tipologia_Corpo_Idrico
from Spatial_Object join EQR_Data on Spatial_Object.monitoringSiteIdentifier = EQR_Data.monitoringSiteIdentifier
where Spatial_Object.lat > 48

2)
select
media_CLASSE_STATO_ECOLOGICO_nord48,
media_CLASSE_STATO_ECOLOGICO_sud48,
percentuale_Natural_nord48,
percentuale_Natural_sud48,
percentuale_HMWB_nord48,
percentuale_HMWB_sud48,
percentuale_AWB_nord48,
percentuale_AWB_sud48
from ( select
avg(UltimaAnalisiEQR.resultEcologicalStatusClassValue) as media_CLASSE_STATO_ECOLOGICO_nord48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'Natural')::int) * 100.0 / count(*) as percentuale_Natural_nord48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'HMWB')::int) * 100.0 / count(*) as percentuale_HMWB_nord48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'AWB')::int) * 100.0 / count(*) as percentuale_AWB_nord48
from Spatial_Object join ( select monitoringSiteIdentifier, resultEcologicalStatusClassValue, parameterNaturalAWBHMWB,
row_number() over (partition by monitoringSiteIdentifier order by phenomenonTimeReferenceYear desc)
as Ordine_Date from EQR_Data where phenomenonTimeReferenceYear is not NULL ) as UltimaAnalisiEQR on
Spatial_Object.monitoringSiteIdentifier = UltimaAnalisiEQR.monitoringSiteIdentifier and UltimaAnalisiEQR.Ordine_Date = 1
where Spatial_Object.lat > 48 and Spatial_Object.confidentialityStatus = 'F' ) as NORD48,
(
select
avg(UltimaAnalisiEQR.resultEcologicalStatusClassValue) as media_CLASSE_STATO_ECOLOGICO_sud48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'Natural')::int) * 100.0 / count(*) as percentuale_Natural_sud48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'HMWB')::int) * 100.0 / count(*) as percentuale_HMWB_sud48,
sum((UltimaAnalisiEQR.parameterNaturalAWBHMWB = 'AWB')::int) * 100.0 / count(*) as percentuale_AWB_sud48
from Spatial_Object join ( select 
monitoringSiteIdentifier,
resultEcologicalStatusClassValue,
parameterNaturalAWBHMWB,
row_number() over (partition by monitoringSiteIdentifier order by phenomenonTimeReferenceYear desc) as Ordine_Date
from EQR_Data where phenomenonTimeReferenceYear is not NULL) as UltimaAnalisiEQR on
Spatial_Object.monitoringSiteIdentifier = UltimaAnalisiEQR.monitoringSiteIdentifier and UltimaAnalisiEQR.Ordine_Date = 1
where Spatial_Object.lat <= 48 and Spatial_Object.confidentialityStatus = 'F') as SUD48

3)
select Spatial_Object.monitoringSiteName, Spatial_Object.surfaceWaterBodyTypeCode, EQR_Data.phenomenonTimeReferenceYear
from Spatial_Object left join EQR_Data on Spatial_Object.monitoringsiteidentifier = EQR_Data.monitoringsiteidentifier

4)
select 
a.countryCode,
a.observedPropertyDeterminandLabel,
count(*) as count_per_label,
(count(*) * 100.0 / CountryTotals.count_total) as percentuale
from eqr_classification a join
( select countryCode, count(*) as count_total
from eqr_classification
group by
countryCode
) as CountryTotals on a.countryCode = CountryTotals.countryCode
group by
a.countryCode,
a.observedPropertyDeterminandLabel,
CountryTotals.count_total
having (count(*) * 100.0 / CountryTotals.count_total) > 5.0
order by
a.countryCode, count_per_label desc

5)
select rbdidentifier, rbdname,
count(case when reservoir like 'Reservoir%' then 1 else NULL end) as reservoir_count
from spatial_object
group by rbdidentifier, rbdname
order by reservoir_count desc

6)
select
countryCode,
waterbodyName,
subUnitName,
centri_di_ricerca_per_corpo
from ( select 
countryCode,
waterbodyName,
subUnitName,
centri_di_ricerca_per_corpo,
row_number() over (partition by countryCode order by centri_di_ricerca_per_corpo desc) as classifica
from ( select 
countryCode,
waterbodyName,
subUnitName,
count(*) as centri_di_ricerca_per_corpo
from spatial_object
group by 
countryCode, waterbodyName, subUnitName) as CorpiIdrici
) as Classifica_CorpiIdrici
where classifica <= 5
order by 
countryCode, centri_di_ricerca_per_corpo desc

7)
select distinct Spatial_Object.monitoringSiteIdentifier, EQR_Data.resultNormalisedEQRValue,
EQR_Data.resultEcologicalStatusClassValue as Classe_stato_ecologico_associata
from Spatial_Object join EQR_Data on Spatial_Object.monitoringSiteIdentifier = EQR_Data.monitoringSiteIdentifier
where EQR_Data.parameterNaturalAWBHMWB='Natural'

8)
select 
waterbodyname,
avg(eqr_data.resultEcologicalStatusClassValue) as avg_eco_status,
max(eqr_data.phenomenonTimeReferenceYear) as ultimo_campionamento 
from spatial_object join eqr_data on spatial_object.monitoringSiteIdentifier = eqr_data.monitoringSiteIdentifier
where eqr_data.resultNumberOfSamples > 5
group by waterbodyname
having avg(eqr_data.resultEcologicalStatusClassValue) < 2
order by avg_eco_status, ultimo_campionamento desc

9)
select count (*) as numero_fiumi_naturali
from Spatial_Object
where Spatial_Object.specialisedZoneType = 'riverWaterBody' and Spatial_Object.naturalAWBHMWB = 'Natural'
group by Spatial_Object.specialisedZoneType, Spatial_Object.naturalAWBHMWB

10)
select Spatial_Object.countryCode, count (*) as n_laghi
from Spatial_Object
where Spatial_Object.specialisedZoneType = 'lakeWaterBody'
group by Spatial_Object.countryCode
order by n_laghi desc
limit 3
