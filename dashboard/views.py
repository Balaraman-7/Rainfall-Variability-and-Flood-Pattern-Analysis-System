import csv
from django.shortcuts import render
from django.http import HttpResponse
from .services import (
    fetch_data, 
    get_distinct_regions, 
    get_distinct_years, 
    generate_trend_chart, 
    generate_bar_comparison_chart
)
from .flood_logic import detect_flood_risk

def extract_filters(request):
    """Helper to extract common GET filters for NO-JS implementation."""
    return {
        'region': request.GET.get('region', ''),
        'year': request.GET.get('year', ''),
    }

def get_common_context(request):
    """Helper to get distinct region/years for the sidebar/navbar dropdowns"""
    return {
        'regions': get_distinct_regions(),
        'years': sorted(get_distinct_years(), reverse=True),
        'filters': extract_filters(request)
    }

def dashboard_view(request):
    """
    Overview page with raw metrics and a bar chart comparison.
    """
    filters = extract_filters(request)
    df = fetch_data(filters)
    
    context = get_common_context(request)
    
    if not df.empty:
        # Generate basic stats using Pandas
        context['total_records'] = len(df)
        context['mean_annual_rainfall'] = round(df['annual'].mean(), 2)
        context['highest_rainfall_year'] = df.loc[df['annual'].idxmax()]['year']
        
        # Determine highest rainfall region if no region filter applied
        if not filters['region']:
            df_grouped = df.groupby('region')['annual'].mean().reset_index()
            context['highest_rainfall_region'] = df_grouped.loc[df_grouped['annual'].idxmax()]['region']
        
        # Generate a bar chart for regions
        if not filters['region']:
            context['bar_chart'] = generate_bar_comparison_chart(df)

    return render(request, 'dashboard/index.html', context)


def trends_view(request):
    """
    Yearly Trends page. Displays a line chart of rainfall over the years.
    """
    filters = extract_filters(request)
    df = fetch_data(filters)
    
    context = get_common_context(request)
    
    if not df.empty:
        context['trend_chart'] = generate_trend_chart(df)

    return render(request, 'dashboard/trends.html', context)


def flood_analysis_view(request):
    """
    Flood risk analysis using numpy-based statistics.
    """
    filters = extract_filters(request)
    df = fetch_data(filters)
    context = get_common_context(request)

    if not df.empty:
        # Group by region and year, aggregate annual rainfall
        df_agg = df.groupby(['region', 'year'])['annual'].mean().reset_index()
        
        # Apply our Rule-based Flood Logic
        df_risk = detect_flood_risk(df_agg, value_col='annual')
        
        # Summarize risk levels
        risk_summary = df_risk['risk_level'].value_counts().to_dict()
        context['risk_summary'] = risk_summary
        
        # Extract Top 20 High Risk entries
        high_risk_df = df_risk[df_risk['risk_level'] == 'High Risk'].sort_values(by='annual', ascending=False).head(20)
        # Convert back to standard dicts for template
        context['high_risk_entries'] = high_risk_df.to_dict('records')

    return render(request, 'dashboard/flood.html', context)


def interactive_map_view(request):
    """
    Pure CSS Interactive map rendering.
    """
    filters = extract_filters(request)
    df = fetch_data(filters)
    context = get_common_context(request)

    # For the pure CSS map, it's nice to pre-calculate region stats to show on simple tooltip logic
    if not df.empty:
        regional_means = df.groupby('region')['annual'].mean().to_dict()
        context['map_data'] = {k: round(v, 2) for k, v in regional_means.items()}
        
    return render(request, 'dashboard/map.html', context)


def export_report_csv(request):
    """
    Export current filtered data to CSV securely without JS.
    """
    filters = extract_filters(request)
    df = fetch_data(filters)
    
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="rainfall_report.csv"'

    writer = csv.writer(response)
    
    if df.empty:
        writer.writerow(['No data available for the given filters'])
        return response

    # Add filters to top of report
    writer.writerow(['Report configuration:'])
    writer.writerow([f"Region: {filters['region'] or 'All'}"])
    writer.writerow([f"Year: {filters['year'] or 'All'}"])
    writer.writerow([])
    
    # Using Pandas direct to_csv on the HttpResponse wrapper is tricky
    # We will just write headers and values
    writer.writerow(df.columns.tolist())
    for _, row in df.iterrows():
        writer.writerow(row.tolist())

    return response
