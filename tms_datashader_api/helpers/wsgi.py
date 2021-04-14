from werkzeug.wsgi import ClosingIterator
import traceback
import threading

class AfterResponse:
    def __init__(self, app=None, immediate=False):
        self.immediate = immediate
        self.callbacks_lock = threading.RLock()
        self.callbacks = []
        if app:
            self.init_app(app)

    def __call__(self, callback):
        print("Adding callback", len(self.callbacks))
        with self.callbacks_lock:
            self.callbacks.append(callback)
        return callback

    def init_app(self, app):
        # install extension
        app.after_response = self

        # install middleware if we want delayed execution
        if self.immediate == False:
            app.wsgi_app = AfterResponseMiddleware(app.wsgi_app, self)

    def flush(self):
        with self.callbacks_lock:
            cbs = self.callbacks
            self.callbacks = []

        for fn in cbs:
            try:
                fn()
            except Exception:
                traceback.print_exc()

class AfterResponseMiddleware:
    def __init__(self, application, after_response_ext):
        self.application = application
        self.after_response_ext = after_response_ext

    def __call__(self, environ, after_response):
        iterator = self.application(environ, after_response)
        try:
            return ClosingIterator(iterator, [self.after_response_ext.flush])
        except Exception:
            traceback.print_exc()
            return iterator