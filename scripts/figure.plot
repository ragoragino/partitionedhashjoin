set terminal pngcairo  transparent enhanced font "arial,10" fontscale 1.0 size 600, 400 
set border 3 front lt black linewidth 1.000 dashtype solid
set boxwidth 0.75 absolute
set style fill  solid 1.00 border lt -1
set grid nopolar
set grid noxtics nomxtics ytics nomytics noztics nomztics nortics nomrtics \
 nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics
set grid layerdefault   lt 0 linecolor 0 linewidth 0.500,  lt 0 linecolor 0 linewidth 0.500
set key outside right top vertical Left reverse noenhanced autotitle columnhead box lt black linewidth 1.000 dashtype solid
set style histogram columnstacked title textcolor lt -1
set datafile missing '-'
set style data histograms
set xtics border in scale 0,0 nomirror  rotate by -45  autojustify
set xtics  norangelimit 
set xtics   ()
set ytics border in scale 0,0 mirror norotate  autojustify
set ztics border in scale 0,0 nomirror norotate  autojustify
set cbtics border in scale 0,0 mirror norotate  autojustify
set rtics axis in scale 0,0 nomirror norotate  autojustify
set title env_figure_title 
set xlabel "Algorithm" 
set xrange [ * : * ] noreverse writeback
set x2range [ * : * ] noreverse writeback
set ylabel "Duration, in ms" 
set yrange [ 0.00000 : * ] noreverse writeback
set y2range [ * : * ] noreverse writeback
set zrange [ * : * ] noreverse writeback
set cbrange [ * : * ] noreverse writeback
set rrange [ * : * ] noreverse writeback
NO_ANIMATION = 1
plot env_data using 2 ti col, '' using 3 ti col, '' using 4 ti col, '' using 5 ti col, '' using 6 ti col, '' using 7 ti col, '' using 8 ti col, '' using 9 ti col, '' using 10 ti col, '' using 11:key(1) ti col
