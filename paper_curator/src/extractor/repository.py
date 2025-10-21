from typing import Any, Dict, List, Optional, Type
from sqlalchemy.orm import Session
from ..db import BaseRepository

class SQLAlchemyRepository(BaseRepository):
    def __init__(self, session: Session, model: Type[Any]):
        super().__init__(session)
        self.model = model

    def create(self, data: Dict[str, Any]) -> Any:
        instance = self.model(**data)
        self.session.add(instance)
        self.session.commit()
        self.session.refresh(instance)
        return instance
    
    def get_by_id(self, record_id: Any) -> Optional[Any]:
        return self.session.query(self.model).get(record_id)
    
    def update(self, record_id: Any, data: Dict[str, Any]) -> Optional[Any]:
        instance = self.get_by_id(record_id)
        if not instance:
            return None
        for key, value in data.items():
            setattr(instance, key, value)
        self.session.commit()
        self.session.refresh(instance)
        return instance
    
    def delete(self, record_id: Any) -> bool:
        instance = self.get_by_id(record_id)
        if not instance:
            return False
        self.session.delete(instance)
        self.session.commit()
        return True
    
    def list(self, limit: int=100, offset: int=0) -> List[Any]:
        return self.session.query(self.model).limit(limit).offset(offset).all()