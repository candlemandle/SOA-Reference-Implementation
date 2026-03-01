import uuid
from typing import Optional
from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from src.database import get_async_session
from src.models.db import ProductDB
from src.models.generated import ProductCreate, ProductUpdate, ProductResponse, PaginatedProductResponse, ProductStatus
from src.routers.auth import RoleChecker

router = APIRouter()


def product_not_found_response():
    return JSONResponse(status_code=404,
                        content={"error_code": "PRODUCT_NOT_FOUND", "message": "Product not found", "details": None})


def access_denied_response():
    return JSONResponse(status_code=403,
                        content={"error_code": "ACCESS_DENIED", "message": "Access denied", "details": None})


allow_all = RoleChecker(["USER", "SELLER", "ADMIN"])
allow_sellers_admins = RoleChecker(["SELLER", "ADMIN"])


@router.post("", response_model=ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(product_in: ProductCreate, session: AsyncSession = Depends(get_async_session),
                         user: dict = Depends(allow_sellers_admins)):
    new_product = ProductDB(
        name=product_in.name, description=product_in.description, price=product_in.price,
        stock=product_in.stock, category=product_in.category, status=product_in.status,
        seller_id=uuid.UUID(user["sub"]) if user["role"] == "SELLER" else None
    )
    session.add(new_product)
    await session.commit()
    await session.refresh(new_product)
    return new_product


@router.get("/{id}", response_model=ProductResponse)
async def get_product(id: uuid.UUID, session: AsyncSession = Depends(get_async_session),
                      user: dict = Depends(allow_all)):
    product = (await session.execute(select(ProductDB).where(ProductDB.id == id))).scalar_one_or_none()
    if not product:
        return product_not_found_response()
    return product


@router.get("", response_model=PaginatedProductResponse)
async def list_products(page: int = Query(0, ge=0), size: int = Query(20, ge=1), status: Optional[ProductStatus] = None,
                        category: Optional[str] = None, session: AsyncSession = Depends(get_async_session),
                        user: dict = Depends(allow_all)):
    query = select(ProductDB)
    if status: query = query.where(ProductDB.status == status)
    if category: query = query.where(ProductDB.category == category)
    total_elements = (await session.execute(select(func.count()).select_from(query.subquery()))).scalar_one()
    products = (await session.execute(query.offset(page * size).limit(size))).scalars().all()
    return PaginatedProductResponse(items=products, totalElements=total_elements, page=page, size=size)


@router.put("/{id}", response_model=ProductResponse)
async def update_product(id: uuid.UUID, product_in: ProductUpdate, session: AsyncSession = Depends(get_async_session),
                         user: dict = Depends(allow_sellers_admins)):
    product = (await session.execute(select(ProductDB).where(ProductDB.id == id))).scalar_one_or_none()
    if not product:
        return product_not_found_response()
    if user["role"] == "SELLER" and product.seller_id != uuid.UUID(user["sub"]):
        return access_denied_response()

    product.name, product.description, product.price, product.stock, product.category, product.status = \
        product_in.name, product_in.description, product_in.price, product_in.stock, product_in.category, product_in.status
    await session.commit()
    await session.refresh(product)
    return product


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(id: uuid.UUID, session: AsyncSession = Depends(get_async_session),
                         user: dict = Depends(allow_sellers_admins)):
    product = (await session.execute(select(ProductDB).where(ProductDB.id == id))).scalar_one_or_none()
    if not product:
        return product_not_found_response()
    if user["role"] == "SELLER" and product.seller_id != uuid.UUID(user["sub"]):
        return access_denied_response()

    product.status = ProductStatus.ARCHIVED
    await session.commit()
    return None