from django.urls import path
from . import views

app_name = 'dashboard'

urlpatterns = [
    path('', views.dashboard_view, name='index'),
    path('trends/', views.trends_view, name='trends'),
    path('flood-analysis/', views.flood_analysis_view, name='flood'),
    path('map/', views.interactive_map_view, name='map'),
    path('export/', views.export_report_csv, name='export'),
]
