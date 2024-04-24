import pandas as pd
df = pd.read_csv('retail_stores.csv')

timezone_replacements = {
    'America/New_York': 'EST (GMT-05)',       # Eastern Standard Time
    'America/Detroit': 'EST (GMT-05)',        # Eastern Standard Time
    'America/Kentucky/Louisville': 'EST (GMT-05)', # Eastern Standard Time
    'America/Kentucky/Monticello': 'EST (GMT-05)', # Eastern Standard Time
    'America/Indiana/Indianapolis': 'EST (GMT-05)', # Eastern Standard Time
    'America/Indiana/Vincennes': 'EST (GMT-05)', # Eastern Standard Time
    'America/Indiana/Winamac': 'EST (GMT-05)', # Eastern Standard Time
    'America/Indiana/Marengo': 'EST (GMT-05)', # Eastern Standard Time
    'America/Indiana/Petersburg': 'EST (GMT-05)', # Eastern Standard Time
    'America/Indiana/Vevay': 'EST (GMT-05)', # Eastern Standard Time

    'America/Chicago': 'CST (GMT-06)',        # Central Standard Time
    'America/Indiana/Tell_City': 'CST (GMT-06)', # Central Standard Time
    'America/Indiana/Knox': 'CST (GMT-06)',   # Central Standard Time
    'America/Menominee': 'CST (GMT-06)',      # Central Standard Time
    'America/North_Dakota/Center': 'CST (GMT-06)', # Central Standard Time
    'America/North_Dakota/New_Salem': 'CST (GMT-06)', # Central Standard Time
    'America/North_Dakota/Beulah': 'CST (GMT-06)', # Central Standard Time

    'America/Denver': 'MST (GMT-07)',         # Mountain Standard Time
    'America/Boise': 'MST (GMT-07)',          # Mountain Standard Time
    'America/Phoenix': 'MST (GMT-07)',        # Mountain Standard Time (Arizona does not observe DST)

    'America/Los_Angeles': 'PST (GMT-08)',    # Pacific Standard Time
    'America/Anchorage': 'AKST (GMT-09)',     # Alaska Standard Time
    'America/Juneau': 'AKST (GMT-09)',        # Alaska Standard Time
    'America/Sitka': 'AKST (GMT-09)',         # Alaska Standard Time
    'America/Metlakatla': 'AKST (GMT-09)',    # Alaska Standard Time
    'America/Yakutat': 'AKST (GMT-09)',       # Alaska Standard Time
    'America/Nome': 'AKST (GMT-09)',          # Alaska Standard Time
    'America/Adak': 'HST (GMT-10)',          # Hawaii-Aleutian Standard Time (Adak)
    'Pacific/Honolulu': 'HST (GMT-10)',      # Hawaii Standard Time
}

df['timezone'] = df['timezone'].map(timezone_replacements)

reductions = {'EST (GMT-05)': 300, 'CST (GMT-06)': 230, 'PST (GMT-08)': 80, 'MST (GMT-07)': 0}
for timezone, reduction in reductions.items():
    # Filter stores in the timezone
    tz_stores = df[df['timezone'] == timezone]
    # Randomly select stores to drop
    drop_indices = tz_stores.sample(n=reduction, random_state=1).index
    # Drop the selected stores
    df = df.drop(drop_indices)

df.to_csv('retail_stores_sm.csv', index=False)

