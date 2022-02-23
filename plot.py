import matplotlib.pyplot as plt
import numpy as np

def line3_join_efficiency():
	durability = [800, 850, 900, 950, 980]
	baseline_time = [800, 99.0889, 9.68136, 0.300903, 0.042376]
	target_time = [303.047, 61.0721, 5.80316, 0.217748, 0.043697]
	join_size = [104471585, 18843268, 1671111, 27911, 123]

	plt.figure()
	plt.bar(durability, join_size, width=20)
	plt.yscale("log")
	plt.xlabel("durability")
	plt.ylabel("join size")
	plt.show()

	plt.figure()
	plt.plot(durability, baseline_time, '-o', color='black', label="Pairwise Join")
	plt.plot(durability, target_time, '-*', color='r', label="Line-3 Join")
	plt.ylim([0, 600])
	plt.xlabel("durability")
	plt.ylabel("time (second)")
	plt.legend()
	plt.show()


def hierarchical_join_efficiency():
	durability = [600, 650, 700, 750, 800, 850, 900]
	baseline_time = [80.4827, 43.9022, 31.0793, 23.4427, 22.1098, 21.7452, 23.4318]
	target_time = [94.0402, 51.6732, 32.1803, 17.3575, 10.592, 5.39796, 2.35432]
	join_size = [1732045, 457435, 98260,15372, 1611, 71, 1]

	plt.figure()
	plt.bar(durability, join_size, width=20)
	plt.yscale("log")
	plt.xlabel("durability")
	plt.ylabel("join size")
	plt.show()

	plt.figure()
	plt.plot(durability, baseline_time, '-o', color='black', label="Pairwise Join")
	plt.plot(durability, target_time, '-*', color='r', label="Hierarchical Join")
	plt.yscale("log")
	plt.xlabel("durability")
	plt.ylabel("time (second)")
	plt.legend()
	plt.show()

def hierarchical_join_efficiency_v2():
	durability = [600, 650, 700, 750, 800, 850, 900]
	baseline_time = [71.8103, 31.132, 13.8359, 6.34637, 2.52598, 0.871666, 0.382696]
	baseline_plus_time = [285.512, 96.7624, 32.3122, 10.1205, 3.03089, 0.918017, 0.419617]
	# target_time = [98.3035, 53.7154, 30.4139, 17.1476, 9.30904, 2.6022, 0.998551]
	# target_plus_time = [307.058, 258.82, 238.92, 230.881, 223.28, 156.812, 156.431]
	target_time = [71.3036, 37.9055, 19.415, 10.4961, 5.95759, 3.10208, 0.875369]
	target_plus_time = [205.398, 171.406, 156.073, 150.357, 148.426, 147.371, 146.886]
	join_size = [1732045, 457435, 98260, 15372, 1611, 71, 1]

# 	600:enum/output/sorting/update/delete time usage: 20.4482/28.1804/19.0844/12.2927/10.3824
# total time usage: 71.3036
# 	650:enum/output/sorting/update/delete time usage: 10.4165/9.67708/14.2549/8.91614/8.89584
# total time usage: 37.9055
# 	700:enum/output/sorting/update/delete time usage: 4.72231/2.55848/10.4726/6.29484/5.83932
# total time usage: 19.415
# 	750:enum/output/sorting/update/delete time usage: 2.23568/0.505995/7.31103/4.1627/3.59173
# total time usage: 10.4961
# 	800:enum/output/sorting/update/delete time usage: 1.162/0.09104/4.4596/2.54383/2.16072
# total time usage: 5.95759
# 	850:enum/output/sorting/update/delete time usage: 0.554615/0.030342/2.40415/1.34122/1.1759
# total time usage: 3.10208
# 	900:num/output/sorting/update/delete time usage: 0.133815/4.5e-05/0.70676/0.404705/0.336804
# total time usage: 0.875369


# 	600:enum/output/sorting/update/delete time usage: 69.3137/20.6421/78.2644/46.0293/69.4134
# total time usage: 205.398
# 	650:enum/output/sorting/update/delete time usage: 49.0234/7.06362/78.3922/45.8445/69.4746
# total time usage: 171.406
# 	700:enum/output/sorting/update/delete time usage: 38.9623/2.13604/79.1104/46.0049/68.9697
# total time usage: 156.073
# 	750:enum/output/sorting/update/delete time usage: 34.4683/0.864968/78.3742/45.945/69.0791
# total time usage: 150.357
# 	800:enum/output/sorting/update/delete time usage: 32.7214/0.531001/78.3858/46.1335/69.0403
# total time usage: 148.426
# 	850:enum/output/sorting/update/delete time usage: 31.9121/0.369849/78.4894/46.262/68.8275
# total time usage: 147.371
# 	900:enum/output/sorting/update/delete time usage: 31.492/0.000238/78.5376/46.413/68.9812
# total time usage: 146.886

	# plt.figure()
	# plt.bar(durability, join_size, width=20)
	# plt.yscale("log")
	# plt.xlabel("durability")
	# plt.ylabel("join size")
	# plt.show()

	plt.figure()
	plt.plot(durability, baseline_plus_time, '-s', color='b', label="Pairwise Join (no filter)")
	plt.plot(durability, target_plus_time, '-^', color='g', label="Hierarchical Join (no filter)")
	plt.plot(durability, baseline_time, '-o', color='black', label="Pairwise Join")
	plt.plot(durability, target_time, '-*', color='r', label="Hierarchical Join")
	# plt.yscale("log")
	plt.xlabel("durability")
	plt.ylabel("time (second)")
	plt.legend()
	plt.show()

def taxi_star_join_filtered():
# 	5000:enum/output/sorting/update/delete time usage: 14.1576/3.15303/13.9025/9.83695/15.714
# total time usage: 42.8616
# ground truth size: 429487 time usage: 440.312
# 	10000:enum/output/sorting/update/delete time usage: 0.202068/0.007341/0.361624/0.304585/0.387712
# total time usage: 0.901706
# ground truth size: 958 time usage: 0.430715
# 	15000:enum/output/sorting/update/delete time usage: 0.100325/0.002479/0.18113/0.173135/0.222559
# total time usage: 0.498498
# ground truth size: 266 time usage: 0.161596
# 	20000:enum/output/sorting/update/delete time usage: 0.077571/0.001338/0.140767/0.142371/0.186506
# total time usage: 0.407786
# ground truth size: 132 time usage: 0.112981
# 	25000:enum/output/sorting/update/delete time usage: 0.069279/0.001136/0.121494/0.130182/0.17485
# total time usage: 0.375447
# ground truth size: 103 time usage: 0.088134
# 	30000:enum/output/sorting/update/delete time usage: 0.06228/0.000775/0.10523/0.119846/0.16288
# total time usage: 0.345781
# ground truth size: 67 time usage: 0.078045
# 	35000:enum/output/sorting/update/delete time usage: 0.056636/0.000539/0.096421/0.111738/0.152764
# total time usage: 0.321677
# ground truth size: 41 time usage: 0.069349
# 	40000:enum/output/sorting/update/delete time usage: 0.053324/0.000467/0.093479/0.106824/0.146871
# total time usage: 0.307486
# ground truth size: 34 time usage: 0.061992
# 	45000:enum/output/sorting/update/delete time usage: 0.052392/0.000332/0.090206/0.10762/0.146709
# total time usage: 0.307053
# ground truth size: 25 time usage: 0.057611
# 	50000:enum/output/sorting/update/delete time usage: 0.049068/0.000285/0.096424/0.102393/0.138076
# total time usage: 0.289822
# ground truth size: 22 time usage: 0.056809

	durability = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000]
	baseline_time = [440.312, 0.430715, 0.161596, 0.112981, 0.088134, 0.078045, 0.069349, 0.061992, 0.057611, 0.056809]
	target_time = [42.8616, 0.901706, 0.498498, 0.407786, 0.375447, 0.345781, 0.321677, 0.307486, 0.307053, 0.289822]
	join_size = [429487, 958, 266, 132, 103, 67, 41, 34, 25, 22]

	plt.figure()
	plt.plot(durability, baseline_time, '-o', color='black', label="Pairwise Join")
	plt.plot(durability, target_time, '-*', color='r', label="Star Join")
	plt.xlabel("durability")
	plt.yscale("log")
	plt.ylabel("time (second)")
	plt.legend()
	plt.show()


def taxi_star_join():
# 	5000: enum/output/sorting/update/delete time usage: 123.75/4.27042/77.6683/37.9675/74.4823
# total time usage: 240.47
# 	ground truth size: 429487 time usage: 4871.6
# 	10000: enum/output/sorting/update/delete time usage: 69.2295/0.315/77.4678/37.2958/72.9647
# total time usage: 179.805
# 	ground truth size: 958 time usage: 2099.72
# 	15000: enum/output/sorting/update/delete time usage: 64.4348/0.266458/77.6921/37.3855/72.9975
# total time usage: 175.084
# 	ground truth size: 266 time usage: 2054.43
# 	20000: enum/output/sorting/update/delete time usage: 62.0597/0.243682/77.5765/37.1356/72.5969
# total time usage: 172.036
# 	ground truth size: 132 time usage: 2048.37
# 	25000: enum/output/sorting/update/delete time usage: 60.1841/0.229524/77.6902/36.8024/72.018
# total time usage: 169.234
# 	ground truth size: 103 time usage: 2044.52
# 	30000: enum/output/sorting/update/delete time usage: 58.9512/0.222273/77.6359/36.5136/71.7406
# total time usage: 167.428
# 	ground truth size: 67 time usage: 2040.02
# 	35000: enum/output/sorting/update/delete time usage: 58.0894/0.213981/77.7643/36.5084/71.7279
# total time usage: 166.54
# 	ground truth size: 41 time usage: 2039.25
# 	40000: enum/output/sorting/update/delete time usage: 57.2587/0.205164/77.5122/36.5128/71.611
# total time usage: 165.588
# 	ground truth size: 34 time usage: 2042.43
# 	45000: enum/output/sorting/update/delete time usage: 56.6259/0.198829/77.6312/36.4645/71.6483
# total time usage: 164.938
# 	ground truth size: 25 time usage: 2039.07
# 	50000: enum/output/sorting/update/delete time usage: 54.8393/0.189434/75.3494/35.8768/70.3472
# total time usage: 161.253
# 	ground truth size: 22 time usage: 2089.96

	durability = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000]
	baseline_time = [4871.6, 2099.72, 2054.43, 2048.37, 2044.52, 2040.02, 2039.25, 2042.43, 2039.07, 2089.96]
	target_time = [240.47, 179.805, 175.084, 172.036, 169.234, 167.428, 166.54, 165.588, 164.938, 161.253]
	join_size = [429487, 958, 266, 132, 103, 67, 41, 34, 25, 22]

	plt.figure()
	plt.bar(durability, join_size, width=1000)
	plt.yscale("log")
	plt.xlabel("durability")
	plt.ylabel("join size")
	plt.show()

	plt.figure()
	plt.plot(durability, baseline_time, '-o', color='black', label="Pairwise Join")
	plt.plot(durability, target_time, '-*', color='r', label="Star Join")
	plt.xlabel("durability")
	plt.yscale("log")
	plt.ylabel("time (second)")
	plt.legend()
	plt.show()

def syn_line_plot():
	durability = [100,200,400,600,800,1000]

	sw_time = [26.3947,24.3784,21.4809,18.8981,16.6738,14.0273]
	hybrid_time = [65.8311,62.7782,54.5026,47.3631,40.3433,33.4521]
	interval_time = [2.11624,2.07381,1.96728,1.86557,1.75954,1.66506]
	baseline_time = [74.4242,71.0774,62.9196,54.3001,46.812,39.6665]

	sw_mem = np.array([1236,1236,1236,1232,1224,1228]) / 1000
	hybrid_mem = np.array([4246380,4008156,3549332,3112516,2703808,2564656]) / 1000
	interval_mem = np.array([1228,1236,1228,1236,1232,1232]) / 1000
	baseline_mem = np.array([969188,916128,829068,715152,623728,538096]) / 1000

	plt.figure()
	plt.plot(durability, sw_mem, '-s', linestyle='dotted', color='red', linewidth=2, markersize=12, label="TimeFirst")
	plt.plot(durability, hybrid_mem, '-d', linestyle='dashed', color='blue', linewidth=2, markersize=12, label="Hybrid")
	plt.plot(durability, interval_mem, '-^', linestyle='dashdot', color='y', linewidth=2, markersize=12, label="Hybrid-Interval")
	plt.plot(durability, baseline_mem, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.yscale("log")
	plt.xlabel("Line($Q_{L4}$): durability threshold", fontsize=18)
	plt.ylabel("peak memory usage (MB)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

	plt.figure()
	plt.plot(durability, sw_time, '-s', linestyle='dotted', color='red', linewidth=2, markersize=12, label="TimeFirst")
	plt.plot(durability, hybrid_time, '-d', linestyle='dashed', color='blue', linewidth=2, markersize=12, label="Hybrid")
	plt.plot(durability, interval_time, '-^', linestyle='dashdot', color='y', linewidth=2, markersize=12, label="Hybrid-Interval")
	plt.plot(durability, baseline_time, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.xlabel("Line($Q_{L4}$): durability threshold", fontsize=18)
	plt.ylabel("query time (second)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

def syn_star_plot():

	durability = [100,200,400,600,800,1000]

	sw_time = [0.481024,0.476054,0.467797,0.421418,0.42808,0.408787]
	baseline_time = [62.5351,58.1762,52.5069,44.4478,38.9837,33.5596]

	sw_mem = np.array([1236,1228,1240,1228,1240,1236]) / 1000
	baseline_mem = np.array([1009264,931632,827508,729272,634556,547624]) / 1000

	plt.figure()
	plt.plot(durability, sw_mem, '-s', linestyle='dotted', color='red', linewidth=2, markersize=12, label="TimeFirst")
	plt.plot(durability, baseline_mem, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.yscale("log")
	plt.xlabel("Star($Q_{S4}$): durability threshold", fontsize=18)
	plt.ylabel("peak memory usage (MB)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

	plt.figure()
	plt.plot(durability, sw_time, '-s', linestyle='dotted', color='red', linewidth=2, markersize=12, label="TimeFirst")
	plt.plot(durability, baseline_time, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.xlabel("Star($Q_{S4}$): durability threshold", fontsize=18)
	plt.ylabel("query time (second)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

def syn_cyclic_plot():

	durability = [100,200,300,400,500,600]

	hybrid_time = [20.4203,18.25,16.8449,15.0343,13.4425,12.4814]
	baseline_time = [99.4901,91.0641,86.0516,78.0158,72.8183,66.9532]

	hybrid_mem = np.array([1232,1244,1240,1232,1240,1236]) / 1000
	baseline_mem = np.array([925988,872600,823968,802600,726252,680676]) / 1000

	plt.figure()
	plt.plot(durability, hybrid_mem, '-d', linestyle='dashed', color='blue', linewidth=2, markersize=12, label="Hybrid")
	plt.plot(durability, baseline_mem, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.yscale("log")
	plt.xlabel("Cyclic($Q_{C4}$): durability threshold", fontsize=18)
	plt.ylabel("peak memory usage (MB)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

	plt.figure()
	plt.plot(durability, hybrid_time, '-d', linestyle='dashed', color='blue', linewidth=2, markersize=12, label="Hybrid")
	plt.plot(durability, baseline_time, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.xlabel("Cyclic($Q_{C4}$): durability threshold", fontsize=18)
	plt.ylabel("query time (second)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

def flight_plot():

	labels = ['$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$', '$Q_{C_3}$', '$Q_{C_4}$', '$Q_{C_5}$', '$Q_{bowtie}$']
	star = [0.8698953957,0.7693934158,0.6417434313]
	generic = [0.3711689059]
	yannakakis = [2.335145665,2.123144546,1.689006187]
	hybrid = [0.9557505905,0.987067325,0.9805781802,1.060411993,0.5456520641,0.5430585016]
	interval_join = [0.8892576905,1.076890343,0.8396756039]

	x = np.arange(len(labels))  # the label locations
	width = 0.3  # the width of the bars

	fig, ax = plt.subplots()
	star_bar = ax.bar([3,4,5], star, width, color='red')
	generic_bar = ax.bar([6], generic, width, color='blue')
	yannakakis_bar = ax.bar(np.array([0,1,2]) - width, yannakakis, width, color='red', label='TimeFirst')
	hybrid_bar = ax.bar(np.array([0,1,2,7,8,9]), hybrid, width, color='blue', label='Hybrid')
	interval_join_bar = ax.bar(np.array([0,1,2]) + width, interval_join, width, color='y', label='Hybrid-Interval')

	# Add some text for labels, title and custom x-axis tick labels, etc.
	ax.set_ylabel('ratio to Baseline', fontsize=18)
	ax.set_xticks(x)
	ax.set_xticklabels(labels, fontsize=18)
	ax.legend()
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.legend(fontsize=18)
	fig.tight_layout()
	plt.savefig("flight-query-time.pdf")

	plt.show()

def dblp_plot():

	labels = ['$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$']

	yannakakis = [3.004801153,2.654014916,2.272849114]
	star = [1.144243883,0.7944126989,0.4451783975]
	hybrid = [1.23227119,1.232836573,1.076818946]
	interval_join = [0.8774369049,0.9509464559,0.8534226834]

	x = np.arange(len(labels))  # the label locations
	width = 0.3  # the width of the bars

	fig, ax = plt.subplots()
	yannakakis_bar = ax.bar(np.array([0,1,2]) - width, yannakakis, width, color='red', label='TimeFirst')
	hybrid_bar = ax.bar(np.array([0,1,2]), hybrid, width, color='blue', label='Hybrid')
	interval_join_bar = ax.bar(np.array([0,1,2]) + width, interval_join, width, color='y', label='Hybrid-Interval')
	star_bar = ax.bar(np.array([3,4,5]), star, width, color='red')

	# Add some text for labels, title and custom x-axis tick labels, etc.
	ax.set_ylabel('ratio to Baseline', fontsize=18)
	ax.set_xticks(x)
	ax.set_xticklabels(labels, fontsize=18)
	ax.legend()
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.legend(fontsize=18)
	fig.tight_layout()
	plt.savefig("dblp-query-time.pdf")
	plt.show()

def tpc_plot():
	labels = ['$Q_{tpc3}$','$Q_{tpc5}$', '$Q_{tpc9}$', '$Q_{tpc10}$']
	

	yannakakis = [3.346323819,2.03764707,0.2344520359,0.2369616307]
	hybrid = [1.079047883,1.022409159,0.05498607419,0.07194244604]
	interval_join = [0.9697096101,0.5773209584,0.01454857678,0.006971702638]

	x = np.arange(len(labels))  # the label locations
	width = 0.3  # the width of the bars

	fig, ax = plt.subplots()
	yannakakis_bar = ax.bar(x - width, yannakakis, width, color='red', label='TimeFirst')
	hybrid_bar = ax.bar(x, hybrid, width, color='blue', label='Hybrid')
	interval_join_bar = ax.bar(x + width, interval_join, width, color='y', label='Hybrid-Interval')

	# Add some text for labels, title and custom x-axis tick labels, etc.
	ax.set_ylabel('ratio to Baseline (log scale)', fontsize=18)
	ax.set_xticks(x)
	ax.set_xticklabels(labels, fontsize=18)
	ax.legend()
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	plt.legend(fontsize=18)
	plt.yscale("log")
	fig.tight_layout()
	plt.savefig("tpc-query-time.pdf")
	plt.show()

def tpc_scalability_plot():

	size = ['50K', '100K', '250K', '500K', '1M']

	sw = np.array([200953.9131,214363.7243,210136.8244,206792.9492,211129.084]) / 1000
	baseline = np.array([133348.4308,139479.0551,139423.1184,136641.7545,138898.7266]) / 1000

	plt.figure()
	plt.plot(size, sw, '-s', linestyle='dotted', color='red', linewidth=2, markersize=12, label="TimeFirst")
	plt.plot(size, baseline, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	# plt.yscale("log")
	plt.xlabel("TPC-E: data size", fontsize=18)
	plt.ylabel("throughput (K resutls/second)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()

def ldbc_scalability_plot():

	size = ['10K', '50K', '120K', '500K', '1M', '2M']

	sw = np.array([222.7739493,206.2517366,244.4369467,256.3776189,235.3252657,233.3200381])
	baseline = np.array([112.0850029,134.2705038,143.820511,179.4752933,157.2185228,153.2052116])

	plt.figure()
	plt.plot(size, sw, '-^', linestyle='dashdot', color='y', linewidth=2, markersize=12, label="Hybrid-Interval")
	plt.plot(size, baseline, '-o', color='black', linewidth=2, markersize=12, label="Baseline")
	plt.xticks(fontsize=15)
	plt.yticks(fontsize=15)
	# plt.yscale("log")
	plt.xlabel("LDBC: data size", fontsize=18)
	plt.ylabel("throughput (K resutls/second)", fontsize=18)
	plt.legend(fontsize=15)
	plt.tight_layout()
	plt.show()


if __name__ == '__main__':
	syn_line_plot()
	syn_star_plot()
	syn_cyclic_plot()
	#flight_plot()
	#dblp_plot()
	# tpc_plot()
	tpc_scalability_plot()
	ldbc_scalability_plot()

