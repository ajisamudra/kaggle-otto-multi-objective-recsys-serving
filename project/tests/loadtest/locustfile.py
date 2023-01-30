"""Locustfile module to perform loadtest using locust"""

from locust import HttpUser, TaskSet, between, task


payload = {
  "aids": [429240,588903,170046,429240,170046],
  "timestamps":  [1674210000,1674220000,1674830000,1674832100,1674832437],
  "event_types": [0,1,1,1,2]
}


class PredictTaskSet(TaskSet):
    def __init__(self, parent):
        super().__init__(parent)
        self.headers = {
            "Accept-Charset": "utf-8",
            "Cache-Control": "no-store",
        }

    @task
    def predict(self):
        route = "/predict"
        with self.client.post(
            route,
            json=payload,
            headers=self.headers,
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                message = f"status code: {resp.status_code}"
                resp.failure(message)


class LocustTest(HttpUser):
    tasks = [PredictTaskSet]
    wait_time = between(0.35, 0.55)
