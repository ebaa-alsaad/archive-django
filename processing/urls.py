from django.urls import path
from . import views

app_name = 'processing'

urlpatterns = [
    # Upload & Processing
    path('uploads/', views.upload_list, name='upload_list'),
    path('uploads/create/', views.upload_create, name='upload_create'),
    path('uploads/<int:upload_id>/', views.upload_detail, name='upload_detail'),
    path('uploads/<int:upload_id>/download/', views.download_file, name='download_file'),
    path('uploads/<int:upload_id>/status/', views.check_status, name='check_status'),
    path('uploads/<int:upload_id>/download_zip/', views.download_zip, name='download_zip'),
    path('uploads/<int:upload_id>/delete/', views.upload_delete, name='upload_delete'),
    
    # Group specific
    path('uploads/<int:upload_id>/groups/<int:group_id>/download/', 
         views.download_group_file, 
         name='download_group'),

    # Dashboard
    path('dashboard/', views.dashboard_view, name='dashboard'),
]