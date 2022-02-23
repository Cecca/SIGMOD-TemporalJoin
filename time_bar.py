import matplotlib.pyplot as plt
import numpy as np


labels = ['$Q_{tpc3}$','$Q_{tpc5}$', '$Q_{tpc9}$', '$Q_{tpc10}$', '$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$', '$Q_{C_3}$', '$Q_{C_4}$', '$Q_{C_5}$', '$Q_{bowtie}$', '$Q_{L_3}$', '$Q_{L_4}$', '$Q_{L_5}$', '$Q_{S_3}$', '$Q_{S_4}$', '$Q_{S_5}$']


x = np.arange(len(labels))  # the label locations
width = 0.2  # the width of the bars 
fig, ax = plt.subplots()
textures = ["///", "\\\\\\", "o", "."]

yannakakis = [3.346323819,2.03764707,0.2344520359,0.2369616307]
hybrid = [1.079047883,1.022409159,0.05498607419,0.07194244604]
interval_join = [0.9697096101,0.5773209584,0.01454857678,0.006971702638]



hybrid_bar = ax.bar(np.array([0,1,2,3]), hybrid, width, color='blue', label='Hybrid', hatch = textures[1])
interval_join_bar = ax.bar(np.array([0,1,2,3]) + width, interval_join, width, color='y', label='Hybrid-Interval', hatch = textures[2])
yannakakis_bar = ax.bar(np.array([0,1,2,3]) - width, yannakakis, width, color='red', label='TimeFirst', hatch = textures[0])

star = [0.8698953957,0.7693934158,0.6417434313]
# generic = [0.3711689059]
generic = [0.004/1.07776]
yannakakis = [2.335145665,2.123144546,1.689006187]
# hybrid = [0.9557505905,0.987067325,0.9805781802,1.060411993,0.5456520641,0.5430585016]
hybrid = [0.9557505905,0.987067325,0.9805781802,0.709456/1.10099, 0.986355/5.73939, (0.004*2+0.040183)/5.39336]
interval_join = [0.8892576905,1.076890343,0.8396756039]

star_bar = ax.bar(np.array([3,4,5]) + 4, star, width, color='red', hatch = textures[0])
generic_bar = ax.bar(np.array([6]) + 4, generic, width, color='blue', hatch = textures[1])
yannakakis_bar = ax.bar(np.array([0,1,2]) + 4 - width, yannakakis, width, color='red', hatch = textures[0])
hybrid_bar = ax.bar(np.array([0,1,2,7,8,9]) + 4, hybrid, width, color='blue', hatch = textures[1])
interval_join_bar = ax.bar(np.array([0,1,2]) + 4 + width, interval_join, width, color='y', hatch = textures[2])

yannakakis = [3.004801153,2.654014916,2.272849114]
star = [1.144243883,0.7944126989,0.4451783975]
hybrid = [1.23227119,1.232836573,1.076818946]
interval_join = [0.8774369049,0.9509464559,0.8534226834]

join_first_flight = [19.32/0.43, 458.72/0.86, 4800/2.08, 24.81/0.42, 1486.57/2.58, 4800/8.73]
join_first_dblp = [0.038/0.15, 1.04/0.37, 35.39/1.43, 0.077/0.59, 6.55/5.27, 735.94/65.26]
join_fisrt_generic_join = [0.004/1.07776, 0.05/1.10099, 1.6/5.73939, 0.5/5.39336]

yannakakis_bar = ax.bar(np.array([0,1,2]) + 14 - width, yannakakis, width, color='red', hatch = textures[0])
hybrid_bar = ax.bar(np.array([0,1,2])  +14, hybrid, width, color='blue', hatch = textures[1])
interval_join_bar = ax.bar(np.array([0,1,2]) + 14+ width, interval_join, width, color='y', hatch = textures[2])
star_bar = ax.bar(np.array([3,4,5]) + 14 , star, width, color='red', hatch = textures[0])

join_first_dblp_bar = ax.bar(np.array([4,5,6,7,8,9]) + 2 * width, join_first_dblp, width, color='grey', label = "JoinFirst", hatch = textures[3])
join_first_flight_bar = ax.bar(np.array([14,15,16,17,18,19]) + 2 * width, join_first_flight, width, color='grey', hatch = textures[3])
join_fisrt_generic_bar = ax.bar(np.array([10,11,12,13]) + 2 * width, join_fisrt_generic_join, width, color='grey', hatch = textures[3])
#ax.text(0.13, -0.15, 'TPC-BiH', ha='center', fontsize=15, transform=ax.transAxes)
#ax.text(0.45, -0.15, 'Flight', ha='center', fontsize=15, transform=ax.transAxes)
#ax.text(0.84, -0.15, 'DBLP', ha='center', fontsize=15, transform=ax.transAxes)


ax.set_ylabel('Ratio to Baseline', fontsize=12)
ax.set_xticks(x)
ax.set_yscale('log')
ax.set_xticklabels(labels, fontsize=15)
ax.legend(fontsize=12, ncol = 2)
plt.xticks(fontsize=15)
plt.yticks(fontsize=12)
plt.tight_layout()
plt.savefig("dblp-query-time-v2.pdf", bbox_inches='tight')
plt.show()





