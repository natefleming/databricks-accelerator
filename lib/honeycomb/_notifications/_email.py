from honeycomb.notification import SendEmailNotification, Notification, Notifier, NotificationProviderFactory, NotificationProvider, NotificationProviderContext

from typing import List
import requests


class _SendEmailNotificationProvider(NotificationProvider):

    def __init__(self, url: str):
        self._url = url

    def can_handle(self, sender: object, notification: Notification) -> bool:
        return type(notification) in [SendEmailNotification]

    def dispatch(self, notification: SendEmailNotification) -> None:
        print(
            f'_SendEmailNotificationProvider.dispatch(notification: {notification})'
        )
        payload = {
            'message': notification.message,
            'subject': notification.subject,
            'receiver': ','.join(notification.recipients)
        }
        if self._url is None:
            print(
                '_SendEmailNotificationProvider is missing configuration for URL'
            )
            return
        print(f'Sending url: {self._url}')
        print(f'Sending email: {payload}')
        response: requests.models.Response = requests.post(self._url,
                                                           json=payload)
        print(f'status_code: {response.status_code}')


class _SendEmailNotificationProviderFactory(NotificationProviderFactory):

    def create(self,
               context: NotificationProviderContext) -> NotificationProvider:
        print(
            f'_SendEmailNotificationProviderFactory.create(context: {context})')
        return _SendEmailNotificationProvider(context.email_endpoint)
