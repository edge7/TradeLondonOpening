#!/bin/bash

path="http://www.truefx.com/dev/data/"
cross="EURUSD"
nameFileTot=$cross"_2009To2015"
declare -A nameToNumberMonth=( ["january"]="01" ["february"]="02" ["march"]="03" ["april"]="04" ["may"]="05" ["june"]="06" ["july"]="07" ["august"]="08" ["september"]="09" ["october"]="10" ["november"]="11" ["december"]="12")
echo ${nameToNumberMonth["january"]}
echo $str | awk '{print toupper($0)}'
for year in 2009 2010 2011 2012 2013 2014 2015 
do
	for month in january february march april may june july august september october november december
	do
			echo " age and month " $year $month ${nameToNumberMonth["$month"]}
			monthUpper=$(echo $month| awk '{print toupper($0)}')
			monthN=${nameToNumberMonth["$month"]}
			pathToDownload=$path$year"/"$monthUpper"-"$year"/"$cross"-"$year"-"$monthN".zip"
			status=$(curl -s --head -w %{http_code} $pathToDownload -o /dev/null)
			echo $status
			if [ "$status" == "200" ]
			then
				wget $pathToDownload
				nameFile=$cross"-"$year"-"$monthN".zip"
				nameFileToCopy=$cross"-"$year"-"$monthN".csv"
				unzip $nameFile
				cat $nameFileToCopy >> $nameFileTot
		    fi
			echo $pathToDownload
			
	done

done
