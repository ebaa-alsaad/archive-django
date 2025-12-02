from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from processing import views
from django.contrib.auth.views import LogoutView
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('', RedirectView.as_view(url='/login/', permanent=False)),
    path('admin/', admin.site.urls),
    
    # Authentication
    path('login/', views.login_view, name='login'),
    path('logout/', LogoutView.as_view(next_page='/login/'), name='logout'),
    path('register/', views.register_view, name='register'),
    
    # Dashboard 
    path('dashboard/', views.dashboard_view, name='dashboard'),
    
    # Processing app
    path('', include(('processing.urls', 'processing'), namespace='processing')),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)