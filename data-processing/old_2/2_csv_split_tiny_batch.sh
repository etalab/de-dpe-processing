totalstart=$(date +%s.%N)

for dep in {1..19} {21..97}; do
    start=$(date +%s.%N)

	echo $dep
	rm -rf ../data-processing/geocodage/2-mini-split/$dep
	mkdir ../data-processing/geocodage/2-mini-split/$dep

	split_filter () { { head -n 1 header_file; cat; } > "$FILE"; }; 
	export -f split_filter; 

	tail -n +2 ../data-processing/geocodage/1-mini/td001_dpe-$dep.csv | split -l 100 --filter=split_filter  --additional-suffix=.csv -d - ../data-processing/geocodage/2-mini-split/$dep/split_td001_dpe-$dep-

        end=$(date +%s.%N)
        runtime=$(python -c "print(${end} - ${start})")
        echo "Runtime for $year was $runtime"

	
	rm -rf ../data-processing/geocodage/3-mini-split-geocoded/$dep
	mkdir ../data-processing/geocodage/3-mini-split-geocoded/$dep

done
totalend=$(date +%s.%N)
totalruntime=$(python -c "print(${totalend} - ${totalstart})")
echo "Total Runtime was $totalruntime"
