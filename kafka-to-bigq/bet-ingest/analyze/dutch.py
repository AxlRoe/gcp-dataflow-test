import numpy as np
from matplotlib import pyplot
from matplotlib.dates import DateFormatter

def dutch (quotes, stake):
    perc = [(1/q)*100 for q in quotes]
    probability = np.sum(perc)
    stake_parts = [round((p/probability)*stake) for p in perc]
    profits = [s*q - stake for (s,q) in zip(stake_parts, quotes)]
    return stake_parts, profits, probability

quotes = [2.50, 2.08, 6]
stake_parts, profits, probability = dutch(quotes, 25)
print("quotes", quotes)
print("parts", stake_parts)
print("profits", profits)
print("probability", probability)

#
#
# q1 = []
# q2 = []
# q3 = []
# q4 = []
#
# fig, axes = pyplot.subplots(nrows=1, ncols=1, sharex=True, figsize=(15, 5))
# x = np.arange(15,100,5)
# for s in x:
#     stake_parts, profits, probability = dutch(quotes, s)
#     q1.append(profits[0])
#     q2.append(profits[1])
#     q3.append(profits[2])
#     q4.append(profits[3])
#
#
# print(q1)
# print(q2)
# print(q3)
# print(q4)
#
# axes.plot(x, q1, label=quotes[0])
# axes.plot(x, q2, label=quotes[1])
# axes.plot(x, q3, label=quotes[2])
# axes.plot(x, q4, label=quotes[3])
#
#
# # set title and y label
# axes.set_title("profits", fontsize=12)
# axes.set_ylabel("profits")
# axes.legend()
#
# # axes.xaxis.grid(b=True, which='major', color='black', linestyle='--', alpha=1)
# axes.xaxis.grid(b=True, which='major', color='black', linestyle='--', alpha=1)
# pyplot.tight_layout()
# pyplot.show()



