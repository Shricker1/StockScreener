"""SQLAlchemy models for runtime history and factor results."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, Text
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class AnalysisRecord(Base):
    __tablename__ = "analysis_records"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(Text, nullable=False)
    name = Column(Text, nullable=True)
    suggestion = Column(Text, nullable=True)
    confidence = Column(Float, nullable=True)
    reasons = Column(Text, nullable=True)
    report_markdown = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class FactorResult(Base):
    __tablename__ = "factor_results"
    id = Column(Integer, primary_key=True, autoincrement=True)
    factor_name = Column(Text, nullable=False)
    ic_value = Column(Float, nullable=True)
    score = Column(Float, nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

