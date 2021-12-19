from django.urls import path

from bike.views import daily_borrow, kmean_merge, getDetail, getSpaceTime, getSpaceMapData

urlpatterns = [
    path('daily/', daily_borrow),
    path('kmean/', kmean_merge),
    path('detail/', getDetail),
    path('space/', getSpaceTime),
    path('spaceMap/', getSpaceMapData),
    # path('order2', listorders2),
]
