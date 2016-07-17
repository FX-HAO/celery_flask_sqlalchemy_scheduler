# -*- coding: utf-8 -*-

import datetime
import json

import sqlalchemy.event
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.ext.associationproxy import association_proxy
from flask.ext.babel import lazy_gettext as _
from celery import schedules

from app import db
from helpers import (
    get_class_by_modelname,
    get_primary_key_value,
)


class ScheduleTaskAssociation(db.Model):
    __bind_key__ = 'ads'
    __tablename__ = 'schedule_task_association'

    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('celery_schedules.id'))
    discriminator = db.Column(db.String(64))
    discriminator_id = db.Column(BIGINT)
    attribute = db.Column(db.String(32), default='')
    description = db.Column(db.Text, default='')

    @property
    def parent(self):
        return get_class_by_modelname(self.discriminator) \
            .query.get(self.discriminator_id)


class HasScheduleTask(object):
    """
    An instance of HasScheduleTask will create ScheduleTaskAssociation
    to associate with DatabaseSchedulerEntry.
    """

    @property
    def schedule_tasks(self):
        name = self.__class__.__name__
        assocs = ScheduleTaskAssociation.query.filter(
            ScheduleTaskAssociation.discriminator == name
        ).filter(
            ScheduleTaskAssociation.discriminator_id == get_primary_key_value(self)
        )

        return [assoc.schedule_task for assoc in assocs]

    def create_schedule_tasks(self, period_task, attri, desc=None, commit=True):
        association = ScheduleTaskAssociation()
        association.discriminator = self.__class__.__name__
        association.discriminator_id = get_primary_key_value(self)
        association.attribute = attri
        association.description = desc
        period_task.association.append(association)
        db.session.add(period_task)
        if commit:
            db.session.commit()

    def _get_associations(self, attri=None):
        query = ScheduleTaskAssociation.query.filter(
            ScheduleTaskAssociation.discriminator == self.__class__.__name__
        ).filter(
            ScheduleTaskAssociation.discriminator_id == get_primary_key_value(self)
        )
        if attri:
            query = query.filter(ScheduleTaskAssociation.attribute == attri)
        return query.all()

    def get_schedule_tasks(self, attri=None):
        return [assoc.schedule_task for assoc in self._get_associations(attri=attri)]

    def enable_task(self, attri=None):
        """Active all the tasks.

        Args:
            attri (optional): Specific attribute of tasks.
        """
        tasks = self.get_schedule_tasks(attri=attri)
        for task in tasks:
            task.enable = True
            db.session.add(tasks)
        db.session.commit()
        return tasks

    def disable_task(self, attri=None):
        """Pause all the tasks.

        Args:
            attri (optional): Specific attribute of tasks.
        """
        tasks = self.get_schedule_tasks(attri=attri)
        for task in tasks:
            task.enable = False
            db.session.add(task)
            db.session.delete(list(task.association)[0])
        db.session.commit()
        return tasks


class CrontabSchedule(db.Model):
    __bind_key__ = 'ads'
    __tablename__ = 'celery_crontabs'

    id = db.Column(db.Integer, primary_key=True)
    minute = db.Column(db.String(64), default='*')
    hour = db.Column(db.String(64), default='*')
    day_of_week = db.Column(db.String(64), default='*')
    day_of_month = db.Column(db.String(64), default='*')
    month_of_year = db.Column(db.String(64), default='*')

    @property
    def schedule(self):
        return schedules.crontab(minute=self.minute,
                                 hour=self.hour,
                                 day_of_week=self.day_of_week,
                                 day_of_month=self.day_of_month,
                                 month_of_year=self.month_of_year)

    @classmethod
    def from_schedule(cls, dbsession, schedule):
        spec = {'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year}

        query = CrontabSchedule.query
        query = query.filter_by(**spec)
        existing = query.all()
        if not existing:
            return cls(**spec)
        elif len(existing) > 1:
            for i in existing:
                dbsession.delete(i)
            dbsession.commit()
            return cls(**spec)
        else:
            return existing[0]


PERIOD_CHOICES = (('days', _('Days')),
                  ('hours', _('Hours')),
                  ('minutes', _('Minutes')),
                  ('seconds', _('Seconds')),
                  ('microseconds', _('Microseconds')))


class IntervalSchedule(db.Model):
    __bind_key__ = 'ads'
    __tablename__ = 'celery_intervals'

    id = db.Column(db.Integer, primary_key=True)
    every = db.Column(db.Integer, nullable=False)
    period = db.Column(db.String(24))

    @property
    def schedule(self):
        return schedules.schedule(datetime.timedelta(**{self.period: self.every}))

    @classmethod
    def from_schedule(cls, dbsession, schedule, period='seconds'):
        every = max(schedule.run_every.total_seconds(), 0)
        try:
            query = dbsession.query(IntervalSchedule)
            query = query.filter_by(every=every, period=period)
            existing = query.one()
            return existing
        except NoResultFound:
            return cls(every=every, period=period)
        except MultipleResultsFound:
            query = dbsession.query(IntervalSchedule)
            query = query.filter_by(every=every, period=period)
            query.delete()
            dbsession.commit()
            return cls(every=every, period=period)


class DatabaseSchedulerEntry(db.Model):
    __bind_key__ = 'ads'
    __tablename__ = 'celery_schedules'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))
    task = db.Column(db.String(255))
    interval_id = db.Column(db.Integer, db.ForeignKey('celery_intervals.id'))
    crontab_id = db.Column(db.Integer, db.ForeignKey('celery_crontabs.id'))
    arguments = db.Column(db.String(255), default='[]')
    keyword_arguments = db.Column(db.String(255), default='{}')
    queue = db.Column(db.String(255))
    exchange = db.Column(db.String(255))
    routing_key = db.Column(db.String(255))
    expires = db.Column(db.DateTime)
    enabled = db.Column(db.Boolean, default=True)
    last_run_at = db.Column(db.DateTime)
    total_run_count = db.Column(db.Integer, default=0)
    date_changed = db.Column(db.DateTime)

    interval = db.relationship(IntervalSchedule)
    crontab = db.relationship(CrontabSchedule)
    association = db.relationship(
        "ScheduleTaskAssociation", backref="schedule_task")

    parents = association_proxy("association", "parent")

    @property
    def args(self):
        return json.loads(self.arguments)

    @args.setter
    def args(self, value):
        self.arguments = json.dumps(value)

    @property
    def kwargs(self):
        return json.loads(self.keyword_arguments)

    @kwargs.setter
    def kwargs(self, kwargs_):
        self.keyword_arguments = json.dumps(kwargs_)

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        if self.crontab:
            return self.crontab.schedule

    def __str__(self):
        return ("[DatabaseSchedulerEntry] { \n"
                "    id: %s \n"
                "    name: %s \n"
                "    task: %s \n"
                "    date_changed: %s \n"
                "}" % (
                    self.id,
                    self.name,
                    self.task,
                    self.date_changed,
                ))


@sqlalchemy.event.listens_for(DatabaseSchedulerEntry, 'before_insert')
def _set_entry_changed_date_before_insert(mapper, connection, target):
    target.date_changed = datetime.datetime.utcnow()


@sqlalchemy.event.listens_for(DatabaseSchedulerEntry, 'before_update')
def _set_entry_changed_date_before_update(mapper, connection, target):
    target.date_changed = datetime.datetime.utcnow()
