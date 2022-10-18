import pytest
from time import sleep
from numpy import random
from threading import Lock

from honeycomb.notification import Notifier, Notification, NotificationProvider, NotificationProviderContext, SendEmailNotification

lock = Lock()


class DelayNotification(Notification):
    pass


class DelayNotificationProvider(NotificationProvider):

    def can_handle(self, sender: object, notification: Notification) -> bool:
        return type(notification) in [DelayNotification]

    def dispatch(self, notification: Notification) -> None:
        print(
            f'DelayNotificationProvider.dispatch(notification: {notification}')
        sleeptime = random.uniform(2, 4)
        with lock:
            print(f"sleeping for: {sleeptime} seconds")
        sleep(sleeptime)


@pytest.fixture(scope="function")
def notifier() -> Notifier:
    context: NotificationProviderContext = NotificationProviderContext(
        email_endpoint=
        'https://prod-44.eastus2.logic.azure.com/workflows/d077657422bc4156acb8ee0b2ce3489e/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=n-I71WUJtNo857h09tlpeDX9m7Mx99mt8hexz3udMwM'
    )
    notifier: Notifier = Notifier(context).with_provider(
        DelayNotificationProvider())
    return notifier


def test_delay_notification_dispatch(notifier):
    [notifier.dispatch(DelayNotification()) for x in range(1, 10)]
    notifier.await_completion()


def test_send_email_notification_dispatch(notifier):
    [
        notifier.send_email('foo', 'bar', ['noreply@moserit.com'])
        for x in range(0, 10)
    ]
    notifier.await_completion()
