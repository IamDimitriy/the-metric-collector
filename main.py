from datetime import datetime

import pandas as pd

file_path = "data/ActiveUsers.txt"
column_names = ["date", "time", "user_id"]
df = pd.read_csv(file_path, sep=" ", header=None, names=column_names)

df["date"] = pd.to_datetime(df["date"])
df["time"] = pd.to_timedelta(df["time"])

df["datetime"] = df['date'] + df['time']
df.drop(columns=['date', 'time'], inplace=True)

# Daily Active Users(DAU)
dau = df.groupby(df['datetime'].dt.date)['user_id'].nunique().reset_index()
dau.columns = ['date', 'DAU']
print("Количество активных пользователей по дням\n", dau, '\n')

# Weekly Active Users(WAU)
wau = df.groupby(df['datetime'].dt.strftime('%Y-%U'))['user_id'].nunique().reset_index()
wau.columns = ['week', 'WAU']
wau['week'] = pd.to_datetime(wau['week'] + '-0', format='%Y-%U-%w')
print("Количество активных пользователей по неделям\n", wau, '\n')

# Monthly Active Users(MAU)
mau = df.groupby(df['datetime'].dt.to_period('M'))['user_id'].nunique().reset_index()
mau.columns = ['month', 'MAU']
print("Количество активных пользователей по месяцам\n", mau, '\n')

# Sticky_factor
dau['date'] = pd.to_datetime(dau['date'])
sticky_factor = pd.merge(dau, mau, left_on=dau['date'].dt.to_period('M'), right_on=mau['month'])
sticky_factor.drop(columns=['month'], inplace=True)
sticky_factor['stickness'] = (sticky_factor['DAU'] / sticky_factor['MAU']) * 100
sticky_factor.columns = ['month', 'date', 'DAU', 'MAU', 'stickness']
print("Коэффициент 'прилипчивости' \n", sticky_factor, '\n')

# Месяц первой активности каждого пользователя
first_activity_month = df.groupby('user_id')['datetime'].min().dt.to_period('M')
# Количество уникальных пользователей в первом месяце после регистрации
first_month_users = df[df['datetime'].dt.to_period('M').isin(first_activity_month)]['user_id'].nunique()
# DataFrame для хранения Retention Rate по месяцам
retention_df = pd.DataFrame(index=first_activity_month.unique(), columns=['Retention Rate'])

# Retention Rate для каждого месяца
for month in first_activity_month.unique():
    retained_users = df[df['datetime'].dt.to_period('M') == month]['user_id'].nunique()
    retention_rate = (retained_users / first_month_users) * 100
    retention_df.at[month, 'Retention Rate'] = retention_rate
print("Коэффициент возвращаемости(Retention Rate)\n", retention_df, '\n')

# Месяц каждой сессии
df['month'] = df['datetime'].dt.to_period('M')
freq = pd.DataFrame()
# Количество сообщений в каждом месяце
freq['message_count'] = df.groupby(['month']).size()
# Количество уникальных пользователей в каждом месяце
freq['user_count'] = df.groupby('month')['user_id'].nunique()

# Session Frequency для каждого пользователя в каждом месяце
freq['session_frequency'] = freq['message_count'] / freq['user_count']
# freq.columns = ['month', 'message_count', 'user_count', 'session_frequency']
print("Session Frequency(на самом деле Message Frequency)\n", freq, '\n')

dau['date'] = dau['date'].dt.to_period('D')
wau['week'] = wau['week'].dt.to_period('D')
sticky_factor['date'] = sticky_factor['date'].dt.to_period('D')
freq.index = freq.index.to_timestamp()
freq.index = freq.index.strftime('%Y-%m')
retention_df.index = retention_df.index.to_timestamp()
retention_df.index = retention_df.index.strftime('%Y-%m')

dau.to_excel(f"results/dau_{datetime.now()}.xlsx")
wau.to_excel(f"results/wau_{datetime.now()}.xlsx")
mau.to_excel(f"results/mau_{datetime.now()}.xlsx")
sticky_factor.to_excel(f"results/sticky_factor_{datetime.now()}.xlsx")
retention_df.to_excel(f"results/retention_rate_{datetime.now()}.xlsx")
freq.to_excel(f"results/session_frequency_{datetime.now()}.xlsx")
