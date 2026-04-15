import numpy as np

def detect_flood_risk(df, value_col='total_rainfall'):
    """
    Detects flood risk based on statistical deviations using NumPy.
    :param df: Pandas DataFrame containing rainfall data.
    :param value_col: The column containing rainfall values to measure risk against.
    :return: DataFrame with an added 'risk_level' column.
    """
    if df.empty or value_col not in df.columns:
        df['risk_level'] = 'No Data'
        return df

    # Extract NumPy array for faster calculation
    values = df[value_col].to_numpy()
    
    # Calculate Mean and Standard Deviation across the historical input
    mean_rain = np.nanmean(values)
    std_rain = np.nanstd(values)

    # If std is 0 (all values same), avoid /0 issues
    if std_rain == 0:
        df['risk_level'] = 'Low Risk'
        return df

    # Logical Rules
    high_risk_threshold = mean_rain + (1.5 * std_rain)
    moderate_risk_threshold = mean_rain + (0.5 * std_rain)

    # Use NumPy's vectorization to assign classes
    risk_classes = np.where(
        values >= high_risk_threshold,
        'High Risk',
        np.where(
            values >= moderate_risk_threshold,
            'Moderate Risk',
            'Low Risk'
        )
    )
    
    df['risk_level'] = risk_classes
    return df
