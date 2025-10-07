import json
import matplotlib.pyplot as plt
from datetime import date
from collections import defaultdict
import numpy as np
import matplotlib.dates as mdates
import matplotlib.cm as cm

with open('step4.json', 'r') as f:
    data = json.load(f)

stock_data = defaultdict(dict)
for stock, records in data.items():
    for record in records:
        d = date(record['year'], record['month'], record['day'])
        stock_data[stock][d] = [record['percent_error'],record['real'], record['guess']]

print(f"Total stocks loaded: {len(stock_data)}")

plt.figure(figsize=(20, 12))

num_stocks = len(stock_data)
colors = cm.get_cmap('tab20', num_stocks)

month_to_errors = defaultdict(list)
month_to_tbill = defaultdict(list)
month_to_guess = defaultdict(list)

for i, (stock, records) in enumerate(stock_data.items()):
    dates = sorted(records.keys())
    errors = [abs(records[d][0]) + 1e-6 for d in dates]  # abs + tiny offset
    tbills = [abs(records[d][1]) + 1e-6 for d in dates]  # abs + tiny offset
    guess = [abs(records[d][2]) + 1e-6 for d in dates]  # abs + tiny offset
    
    # plt.plot(dates, errors, linewidth=0.8, alpha=0.3, color=colors(i))
    
    for d, e, t, g in zip(dates, errors, tbills, guess):
        ym = date(d.year, d.month, 1)
        month_to_errors[ym].append(e)
        month_to_tbill[ym].append(t)
        month_to_guess[ym].append(g)

monthly_dates = sorted(month_to_errors.keys())
# monthly_avg_errors = [np.mean(month_to_errors[m]) for m in monthly_dates]
monthly_avg_tbill = [np.mean(month_to_tbill[m]) for m in monthly_dates]
monthly_avg_guess = [np.mean(month_to_guess[m]) for m in monthly_dates]

# plt.ylim(0, 500)
# plt.plot(monthly_dates, monthly_avg_errors, color='black', linewidth=3, alpha=0.9, label='Monthly Average Absolute Percent Error')
plt.plot(monthly_dates, monthly_avg_tbill, color='black', linewidth=3, alpha=0.9, label='13 week TBill')
plt.plot(monthly_dates, monthly_avg_guess, color='red', linewidth=2.5, alpha=0.9, label='implied discount rate')

plt.xlabel('Date', fontsize=16)
plt.ylabel('Discount Rate', fontsize=16)
plt.title('All Stocks in S&P500 2008-2012, Implied Discount rate vs 13 Week TBill Rate, Step 4: Not cheating with future dividends', fontsize=17)

# plt.yscale('log')

plt.grid(True, linestyle='--', alpha=0.3)

plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=6))
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.xticks(rotation=45)
plt.tight_layout()

plt.legend(fontsize=14)

plt.savefig('step4bettergraph.png', dpi=400)
plt.close()


