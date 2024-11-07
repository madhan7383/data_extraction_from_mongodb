def save_to_csv(df, filename):
    df.to_csv(filename, index=False)

def save_to_excel(df, filename):
    df.to_excel(filename, index=False)
