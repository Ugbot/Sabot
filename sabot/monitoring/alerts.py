#!/usr/bin/env python3
"""
Alerting System for Sabot

Provides intelligent alerting with:
- Configurable alert rules
- Multiple severity levels
- Alert escalation
- Alert aggregation and correlation
- Integration with notification systems
"""

import time
import logging
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """Definition of an alert rule."""
    name: str
    metric_name: str
    condition: str  # e.g., "value > 90", "rate > 10"
    severity: AlertSeverity
    description: str
    labels: Dict[str, str] = field(default_factory=dict)
    cooldown_seconds: float = 300.0  # Don't re-alert for 5 minutes
    enabled: bool = True

    def evaluate(self, metrics_collector) -> Optional['Alert']:
        """Evaluate the rule against current metrics."""
        try:
            # Get metric stats
            stats = metrics_collector.get_metric_stats(self.metric_name)

            if not stats:
                return None

            # Parse condition
            if '>' in self.condition:
                threshold = float(self.condition.split('>')[1].strip())
                current_value = stats.get('current', stats.get('latest', 0))

                if current_value > threshold:
                    return Alert(
                        rule_name=self.name,
                        severity=self.severity,
                        message=f"{self.description}: {current_value} > {threshold}",
                        metric_name=self.metric_name,
                        current_value=current_value,
                        threshold=threshold,
                        labels=self.labels.copy()
                    )

            elif '<' in self.condition:
                threshold = float(self.condition.split('<')[1].strip())
                current_value = stats.get('current', stats.get('latest', 0))

                if current_value < threshold:
                    return Alert(
                        rule_name=self.name,
                        severity=self.severity,
                        message=f"{self.description}: {current_value} < {threshold}",
                        metric_name=self.metric_name,
                        current_value=current_value,
                        threshold=threshold,
                        labels=self.labels.copy()
                    )

            elif 'rate' in self.condition and '>' in self.condition:
                # Rate-based condition
                threshold = float(self.condition.split('>')[1].strip())
                rate = stats.get('rate_per_second', 0)

                if rate > threshold:
                    return Alert(
                        rule_name=self.name,
                        severity=self.severity,
                        message=f"{self.description}: rate {rate:.2f}/s > {threshold}",
                        metric_name=self.metric_name,
                        current_value=rate,
                        threshold=threshold,
                        labels=self.labels.copy()
                    )

        except Exception as e:
            logger.error(f"Failed to evaluate alert rule {self.name}: {e}")

        return None


@dataclass
class Alert:
    """An active alert."""
    rule_name: str
    severity: AlertSeverity
    message: str
    metric_name: str
    current_value: Union[int, float]
    threshold: Union[int, float]
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    resolved: bool = False
    resolved_timestamp: Optional[float] = None

    @property
    def is_active(self) -> bool:
        """Check if alert is currently active."""
        return not self.resolved

    @property
    def duration(self) -> float:
        """Get alert duration in seconds."""
        end_time = self.resolved_timestamp or time.time()
        return end_time - self.timestamp


class AlertManager:
    """
    Manages alerts and notifications for Sabot applications.

    Features:
    - Rule-based alerting
    - Alert aggregation and correlation
    - Configurable notification channels
    - Alert lifecycle management
    """

    def __init__(self):
        self.rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.resolved_alerts: List[Alert] = []
        self.notification_callbacks: List[Callable[[Alert], None]] = []

        # Alert state tracking
        self._last_alert_times: Dict[str, float] = {}

        # Initialize built-in alert rules
        self._initialize_builtin_rules()

    def _initialize_builtin_rules(self):
        """Initialize built-in alert rules."""
        # Memory usage alerts
        self.add_rule(AlertRule(
            name="high_memory_usage",
            metric_name="sabot_memory_usage_percent",
            condition="value > 85",
            severity=AlertSeverity.WARNING,
            description="High memory usage detected",
            labels={"component": "system"}
        ))

        self.add_rule(AlertRule(
            name="critical_memory_usage",
            metric_name="sabot_memory_usage_percent",
            condition="value > 95",
            severity=AlertSeverity.CRITICAL,
            description="Critical memory usage - risk of OOM",
            labels={"component": "system"}
        ))

        # Error rate alerts
        self.add_rule(AlertRule(
            name="high_error_rate",
            metric_name="sabot_errors_total",
            condition="rate > 5",
            severity=AlertSeverity.ERROR,
            description="High error rate detected",
            labels={"component": "application"}
        ))

        # Message processing alerts
        self.add_rule(AlertRule(
            name="low_success_rate",
            metric_name="sabot_messages_failed_total",
            condition="rate > 10",
            severity=AlertSeverity.ERROR,
            description="High message failure rate",
            labels={"component": "streaming"}
        ))

        # Stream processing alerts
        self.add_rule(AlertRule(
            name="no_active_streams",
            metric_name="sabot_active_streams",
            condition="value < 1",
            severity=AlertSeverity.WARNING,
            description="No active streams detected",
            labels={"component": "streaming"}
        ))

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self.rules[rule.name] = rule

    def remove_rule(self, rule_name: str) -> None:
        """Remove an alert rule."""
        if rule_name in self.rules:
            del self.rules[rule_name]

    def add_notification_callback(self, callback: Callable[[Alert], None]) -> None:
        """Add a notification callback."""
        self.notification_callbacks.append(callback)

    def evaluate_rules(self, metrics_collector) -> List[Alert]:
        """Evaluate all alert rules and return triggered alerts."""
        new_alerts = []

        for rule in self.rules.values():
            if not rule.enabled:
                continue

            alert = rule.evaluate(metrics_collector)
            if alert:
                # Check cooldown
                last_alert_time = self._last_alert_times.get(rule.name, 0)
                if time.time() - last_alert_time >= rule.cooldown_seconds:
                    new_alerts.append(alert)
                    self._last_alert_times[rule.name] = time.time()

        return new_alerts

    def process_alerts(self, alerts: List[Alert]) -> None:
        """Process and manage alerts."""
        for alert in alerts:
            # Check if this alert is already active
            alert_key = f"{alert.rule_name}:{alert.metric_name}"

            if alert_key in self.active_alerts:
                # Update existing alert if more severe
                existing = self.active_alerts[alert_key]
                if alert.severity.value > AlertSeverity[existing.severity.value.upper()].value:
                    existing.severity = alert.severity
                    existing.message = alert.message
                    existing.current_value = alert.current_value
            else:
                # New alert
                self.active_alerts[alert_key] = alert

                # Notify subscribers
                self._notify_alert(alert)

    def resolve_alert(self, rule_name: str, metric_name: str) -> None:
        """Resolve an active alert."""
        alert_key = f"{rule_name}:{metric_name}"

        if alert_key in self.active_alerts:
            alert = self.active_alerts[alert_key]
            alert.resolved = True
            alert.resolved_timestamp = time.time()

            # Move to resolved list
            self.resolved_alerts.append(alert)
            del self.active_alerts[alert_key]

            # Notify resolution
            self._notify_alert_resolution(alert)

            # Keep only last 1000 resolved alerts
            if len(self.resolved_alerts) > 1000:
                self.resolved_alerts = self.resolved_alerts[-1000:]

    def _notify_alert(self, alert: Alert) -> None:
        """Notify subscribers of new alert."""
        for callback in self.notification_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert notification callback failed: {e}")

    def _notify_alert_resolution(self, alert: Alert) -> None:
        """Notify subscribers of alert resolution."""
        for callback in self.notification_callbacks:
            try:
                # Create resolution notification
                resolution_alert = Alert(
                    rule_name=alert.rule_name,
                    severity=AlertSeverity.INFO,
                    message=f"ALERT RESOLVED: {alert.message}",
                    metric_name=alert.metric_name,
                    current_value=alert.current_value,
                    threshold=alert.threshold,
                    labels={**alert.labels, "resolution": "true"}
                )
                callback(resolution_alert)
            except Exception as e:
                logger.error(f"Alert resolution notification callback failed: {e}")

    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return list(self.active_alerts.values())

    def get_resolved_alerts(self, limit: int = 100) -> List[Alert]:
        """Get recently resolved alerts."""
        return self.resolved_alerts[-limit:]

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary statistics."""
        active_by_severity = defaultdict(int)
        resolved_by_severity = defaultdict(int)

        for alert in self.active_alerts.values():
            active_by_severity[alert.severity.value] += 1

        for alert in self.resolved_alerts:
            resolved_by_severity[alert.severity.value] += 1

        return {
            'active_alerts': len(self.active_alerts),
            'resolved_alerts': len(self.resolved_alerts),
            'active_by_severity': dict(active_by_severity),
            'resolved_by_severity': dict(resolved_by_severity),
            'most_recent_alerts': [
                {
                    'rule': alert.rule_name,
                    'severity': alert.severity.value,
                    'message': alert.message,
                    'timestamp': alert.timestamp
                }
                for alert in sorted(self.active_alerts.values(),
                                  key=lambda x: x.timestamp, reverse=True)[:5]
            ]
        }

    def clear_resolved_alerts(self, older_than_hours: float = 24.0) -> int:
        """Clear resolved alerts older than specified hours."""
        cutoff_time = time.time() - (older_than_hours * 3600)
        original_count = len(self.resolved_alerts)

        self.resolved_alerts = [
            alert for alert in self.resolved_alerts
            if alert.resolved_timestamp and alert.resolved_timestamp > cutoff_time
        ]

        return original_count - len(self.resolved_alerts)


# Notification helpers

def create_console_alert_handler() -> Callable[[Alert], None]:
    """Create a console alert handler."""
    def console_handler(alert: Alert):
        severity_emoji = {
            AlertSeverity.INFO: "ℹ️",
            AlertSeverity.WARNING: "⚠️",
            AlertSeverity.ERROR: "❌",
            AlertSeverity.CRITICAL: "🚨"
        }

        emoji = severity_emoji.get(alert.severity, "❓")
        print(f"{emoji} ALERT: {alert.message}")

        if alert.labels:
            labels_str = ", ".join(f"{k}={v}" for k, v in alert.labels.items())
            print(f"   Labels: {labels_str}")

    return console_handler


def create_log_alert_handler(logger_name: str = "sabot.alerts") -> Callable[[Alert], None]:
    """Create a logging alert handler."""
    alert_logger = logging.getLogger(logger_name)

    def log_handler(alert: Alert):
        log_level = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.ERROR: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL
        }.get(alert.severity, logging.INFO)

        alert_logger.log(log_level, f"ALERT: {alert.message}", extra={
            'alert_rule': alert.rule_name,
            'severity': alert.severity.value,
            'metric': alert.metric_name,
            'current_value': alert.current_value,
            'threshold': alert.threshold,
            'labels': alert.labels
        })

    return log_handler
