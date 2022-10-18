from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Type, NamedTuple
from collections import namedtuple

from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from queue import Queue, Empty

import honeycomb.utils


class NotificationProviderContext(NamedTuple):
    email_endpoint: str


class Notification(ABC):
    pass


class NotificationProvider(ABC):

    @abstractmethod
    def can_handle(self, sender: object, notification: Notification) -> bool:
        pass

    @abstractmethod
    def dispatch(self, notification: Notification) -> None:
        pass


class NotificationProviderFactory(ABC):

    @abstractmethod
    def create(self,
               context: NotificationProviderContext) -> NotificationProvider:
        pass


class Notifier(object):

    def __init__(self,
                 context: NotificationProviderContext,
                 providers: List[NotificationProvider] = None,
                 num_threads: int = 5) -> None:
        self._executor = ThreadPoolExecutor(num_threads)
        self._dispatcher_thread = None
        self._notifications = Queue()
        self._is_running = False
        self._providers: List[NotificationProvider] = providers
        if providers is None:
            self._providers = self._find_providers(context)
        self.start()

    def with_provider(self, provider: NotificationProvider) -> Notifier:
        self._providers += [provider]
        return self

    def start(self) -> None:
        print('Notifier._initialize')
        self._dispatcher_thread = Thread(target=self._dispatcher, args=())
        self._is_running = True
        self._dispatcher_thread.start()

    def dispatch(self, notification: Notification) -> None:
        print(f'Notifier.dispatch({notification})')
        self._notifications.put(notification)

    def await_completion(self) -> None:
        print('Notifier.await_completion')
        self._is_running = False
        self._dispatcher_thread.join()

    def shutdown_now(self) -> None:
        print('Notifier.shutdown_now')
        self._is_running = False
        self._notifications = Queue()
        self._executor.shutdown()
        self._dispatcher_thread.join()

    def send_email(self, subject: str, message: str,
                   recipients: List[str]) -> None:
        notification = SendEmailNotification(subject, message, recipients)
        self.dispatch(notification)

    def _find_providers(self, context: NotificationProviderContext) -> None:
        print('Notifier._register_providers')
        provider_factories = [
            member() for member in honeycomb.utils.find_members(
                'honeycomb._notifications',
                honeycomb.utils.is_child_of(NotificationProviderFactory))
        ]
        print(f'provider_factories {provider_factories}')
        providers: List[NotificationProvider] = [
            p.create(context) for p in provider_factories
        ]
        print(f'providers {providers}')
        return providers

    def _dispatcher(self) -> None:
        print('Notifier._dispatcher')
        while self._is_running or not self._notifications.empty():
            try:
                notification: Notification = self._notifications.get(block=True,
                                                                     timeout=5)
                for provider in self._providers:
                    if provider.can_handle(self, notification):
                        self._executor.submit(provider.dispatch, (notification))
            except Empty:
                pass
            except Exception as e:
                print(f'An exception has occurred: {e}')
        self._executor.shutdown()
        print('complete')


class SendEmailNotification(Notification):

    def __init__(self, subject: str, message: str, recipients: List[str]):
        self._recipients = recipients
        self._subject = subject
        self._message = message

    @property
    def recipients(self) -> List[str]:
        return self._recipients

    @property
    def subject(self) -> str:
        return self._subject

    @property
    def message(self) -> str:
        return self._message
