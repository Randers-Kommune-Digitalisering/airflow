from unittest.mock import patch, MagicMock

from dags.utils.notifications import send_teams_alert


def test_send_teams_alert():
    mock_context = {
        'dag': type('DAG', (), {'dag_id': 'test_dag'})(),
        'task_instance': type('TaskInstance', (), {
            'task_id': 'test_task',
            'log_url': 'https://mock-log-url'
        })(),
        'execution_date': '2000-01-01T00:00:00'
    }

    mock_conn = MagicMock()
    mock_conn.host = "https://mock-webhook-url"

    with patch('dags.utils.notifications.BaseHook.get_connection', return_value=mock_conn), patch('dags.utils.notifications.requests.post') as mock_post:
        send_teams_alert(mock_context)
        mock_post.assert_called_once()
        args, _ = mock_post.call_args
        assert args[0] == mock_conn.host
