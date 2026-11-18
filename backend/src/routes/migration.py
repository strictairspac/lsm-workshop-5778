import json

from fastapi import APIRouter, Depends, Query, Response

from ..config.database import get_db
from ..schemas.migration import MigrationInput, ShardWriteInput
from ..services.auth import current_user, writer
from ..services import migration as service

router = APIRouter(prefix='/migrations')


@router.get('')
def get(db=Depends(get_db), user=Depends(current_user)):
    return service.listing(db)


@router.post('', status_code=201)
def add(data: MigrationInput, db=Depends(get_db), user=Depends(writer)):
    return service.create(db, data)


@router.get('/{ident}')
def one(ident: int, db=Depends(get_db), user=Depends(current_user)):
    return service.detail(db, ident)


@router.get('/{ident}/export')
def export(ident: int, db=Depends(get_db), user=Depends(current_user)):
    payload = {'migration': service.detail(db, ident), 'ownership': service.ownership(db, ident),
               'snapshot': service.query(db, ident)}
    return Response(json.dumps(payload, ensure_ascii=False, indent=2), media_type='application/json',
                    headers={'Content-Disposition': f'attachment; filename="migration-{ident}.json"'})


@router.post('/{ident}/advance')
def advance(ident: int, db=Depends(get_db), user=Depends(writer)):
    return service.advance(db, ident)


@router.post('/{ident}/resume')
def resume(ident: int, db=Depends(get_db), user=Depends(writer)):
    return service.resume(db, ident)


@router.post('/{ident}/switch')
def switch(ident: int, db=Depends(get_db), user=Depends(writer)):
    return service.switch(db, ident)


@router.post('/{ident}/writes', status_code=201)
def write(ident: int, data: ShardWriteInput, db=Depends(get_db), user=Depends(writer)):
    return service.write(db, ident, data)


@router.get('/{ident}/query')
def query(ident: int, start: str | None = Query(default=None, max_length=64),
          end: str | None = Query(default=None, max_length=64),
          db=Depends(get_db), user=Depends(current_user)):
    return service.query(db, ident, start, end)


@router.get('/{ident}/ownership')
def ownership(ident: int, db=Depends(get_db), user=Depends(current_user)):
    return service.ownership(db, ident)
