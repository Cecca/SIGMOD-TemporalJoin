
import matplotlib.pyplot as plt
import numpy as np

def overall_mem_usage():

	labels = ['$Q_{tpc3}$','$Q_{tpc5}$', '$Q_{tpc9}$', '$Q_{tpc10}$', '$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$', '$Q_{C_3}$', '$Q_{C_4}$', '$Q_{C_5}$', '$Q_{bowtie}$', '$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$']
	x = np.arange(len(labels))  # the label locations
	width = 0.2  # the width of the bars 
	fig, ax = plt.subplots()
	textures = ["///", "\\\\\\", "o", "-"]

	yannakakis =    [868.32/1.23, 873.5/768.1, 1095/5623.0, 1459.0/5644.0]
	hybrid = 	    [819.3/1.23, 840.9/768.1, 1177.0/5623.0, 1735.0/5644.0]
	interval_join = [753.9/1.23, 723.9/768.1, 934.0/5623.0, 1332.0/5644.0]

	hybrid_bar = ax.bar(np.array([0,1,2,3]), hybrid, width, color='blue', label='Hybrid', hatch = textures[1])
	interval_join_bar = ax.bar(np.array([0,1,2,3]) + width, interval_join, width, color='y', label='Hybrid-Interval', hatch = textures[2])
	yannakakis_bar = ax.bar(np.array([0,1,2,3]) - width, yannakakis, width, color='red', label='TimeFirst', hatch = textures[0])

	flight_yannakakis = np.array([ 1256,1252,1256,1256,1256,1826276])
	flight_hybrid = np.array([ 1256,1260,1252, 1256,1252,1252,1260])
	flight_interval_join = np.array([ 1256,1260,1244])
	flight_baseline = np.array([ 1256,1256,1252,1252,1252,2625800,1256,1256,1256,1252])
	flight_join_first = np.array([355488+404,355488+4282108,25769803,25769803,25769803,25769803,355488+404,355488+16914,355488+617004,355488+380064])

	flight_yannakakis_bar = ax.bar(np.array([4,5,6,7,8,9]) - width, flight_yannakakis / flight_baseline[0:6], width, color='red', hatch = textures[0])
	flight_hybrid_bar_1 = ax.bar(np.array([4,5,6]), flight_hybrid[0:3]/flight_baseline[0:3], width, color='blue', hatch = textures[1])
	flight_hybrid_bar_2 = ax.bar(np.array([10,11,12,13]), flight_hybrid[3:]/flight_baseline[6:], width, color='blue', hatch = textures[1])
	flight_interval_join = ax.bar(np.array([4,5,6]) + width, flight_interval_join/flight_baseline[0:3], width, color='y', hatch = textures[2])
	flight_join_first = ax.bar(np.array([4,5,6,7,8,9,10,11,12,13]) + width, flight_join_first/flight_baseline, width, label='JoinFirst', color='lightgrey', hatch = textures[3])

	dblp_yannakakis = np.array([ 3035736,3336752,3817448,1232604,1231188,1219476])
	dblp_hybrid = np.array([3753280,3833288,2912832])
	dblp_interval_join = np.array([ 2924912,1686484,1467592])
	dblp_baseline = np.array([ 3306008,2895016,3787364,1229348,1204732,1265608])
	dblp_join_first = np.array([271024+147358, 25769803, 25769803, 25769803, 25769803, 25769803])

	dblp_baseline_1 = dblp_baseline[0:3]

	dblp_hybrid_bar = ax.bar(np.array([14,15,16]), np.divide(dblp_hybrid, dblp_baseline_1), width, color='blue', hatch = textures[1])
	dblp_interval_join_bar = ax.bar(np.array([14,15,16]) + width, np.divide(dblp_interval_join, dblp_baseline_1), width, color='y', hatch = textures[2])
	dblp_yannakakis_bar = ax.bar(np.array([14,15,16,17,18,19]) - width, np.divide(dblp_yannakakis, dblp_baseline), width, color='red', hatch = textures[0])
	dblp_join_first_bar = ax.bar(np.array([14,15,16,17,18,19]) + 2 * width, dblp_join_first / dblp_baseline, width, color='lightgrey', hatch = textures[3])

	ax.set_ylabel('Ratio to Baseline', fontsize=18)
	ax.set_xticks(x)
	ax.set_yscale('log')
	ax.set_ylim([0, 10000])
	ax.set_xticklabels(labels, fontsize=18)
	ax.legend(fontsize=15, ncol = 1)
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	fig.tight_layout()
	# plt.savefig("memory-tpcbih-v2.pdf", bbox_inches='tight')
	plt.show()

def dblp_flight_mem_usage():
	labels = ['$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$']
	x = np.arange(len(labels))  # the label locations
	width = 0.2  # the width of the bars 
	fig, ax = plt.subplots()
	textures = ["///", "\\\\\\", "o", "-"]

	dblp_yannakakis = np.array([ 3035736,3336752,3817448,1232604,1231188,1219476])
	dblp_hybrid = np.array([3753280,3833288,2912832])
	dblp_interval_join = np.array([ 2924912,1686484,1467592])
	dblp_baseline = np.array([ 3306008,2895016,3787364,1229348,1204732,1265608])

	dblp_baseline_1 = dblp_baseline[0:3]

	dblp_hybrid_bar = ax.bar(np.array([0,1,2]), np.divide(dblp_hybrid, dblp_baseline_1), width, color='blue', label='Hybrid', hatch = textures[1])
	dblp_interval_join_bar = ax.bar(np.array([0,1,2]) + width, np.divide(dblp_interval_join, dblp_baseline_1), width, color='y', label='Hybrid-Interval', hatch = textures[2])
	dblp_yannakakis_bar = ax.bar(np.array([0,1,2,3,4,5]) - width, np.divide(dblp_yannakakis, dblp_baseline), width, color='red', label='TimeFirst', hatch = textures[0])

	# ax.set_ylabel('Ratio to Baseline', fontsize=1)
	ax.set_xticks(x)
	# ax.set_yscale('log')
	ax.set_xticklabels(labels, fontsize=15)
	ax.legend(fontsize=12, ncol = 2)
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=12)
	plt.tight_layout()
	# plt.savefig("dblp-query-time-v2.pdf", bbox_inches='tight')
	plt.show()

if __name__ == '__main__':
	# dblp_flight_mem_usage()
	# tpc_mem_usage()
	overall_mem_usage()





