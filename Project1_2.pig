REGISTER /usr/local/pig/lib/piggybank.jar

dataset_file = LOAD 'Crimes.csv' using org.apache.pig.piggybank.storage.CSVLoader() AS(id:int,Case_Number:chararray,Date1:chararray,Block:chararray,IUCR:int,Primary_Type:chararray,Description:chararray,Location:chararray,Arrest:chararray,Domestic:chararray,Beat:int,District:int,Ward:int,Community_Area:int,FBICode:chararray,X_Coordinate:int,Y_Coordinate:int,Year:int,Updated_On:chararray,Latitude:float,Longitude:float,Location2:chararray);

//no of cases


****

1.
grp_FBICode = GROUP dataset_file by FBICode ;
grp_FBICode1 = FOREACH grp_FBICode GENERATE group,COUNT(dataset_file.id);

dump grp_FBICode1


2.
grp_FBICode2 = FILTER dataset_file by FBICode matches '(32)?';
grp_FBICode3 = FOREACH( GROUP grp_FBICode2 by FBICode ) GENERATE group,COUNT(dataset.id);



3.
grp_FBICode = FILTER dataset_file by Primary_Type matches '(Theft|theft|THEFT)?' AND (Arrest matches '(true)?');
grp_by_district = group grp_FBICode by District;
no_of_arrest = FOREACH grp_by_district GENERATE group,COUNT(grp_FBICode.Arrest);

4.

grp_FBI_CASES = FOREACH(FILTER dataset_file by (Arrest matches '(true)?')) GENERATE id,Case_Number,FLATTEN(STRSPLIT(SUBSTRING(Date1,0,10),'/',3));
grp_FBI_CASES1 = FOREACH grp_FBI_CASES GENERATE $0 as id:chararray,$1 as Case_Number:chararray,$2 as month:int,$3 as date:int,$4 as year:int;
grp_FBI_CASES_date = FOREACH (FILTER grp_FBI_CASES1 by (year == 2014 AND  month  == 10 OR month == 11 or month == 12) ) Generate id;

grp_FBI_CASES_date = FOREACH (FILTER grp_FBI_CASES1 by (year == 2014 AND ( month  == 10 OR month == 11 OR month == 12)) OR (year == 2015 AND (month  == 01 OR month == 02 OR month == 03 OR month == 04 OR month == 05 OR month == 06 OR month == 07 OR month == 08 OR month == 09 OR month == 10))) Generate id;





dump grp_FBI_CASES_date


grp_FBI_CASES_date = FOREACH(FILTER grp_FBI_CASES by Date1 matches (([0-9]?(/)?[0-9]?(/)?(2014|2015))) GENERATE id,Case_Number;


