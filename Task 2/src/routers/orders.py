import uuid
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, status, Path
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from src.database import get_async_session
from src.models.db import ProductDB, OrderDB, OrderItemDB, UserOperationDB, PromoCodeDB
from src.models.generated import OrderCreate, OrderUpdate, OrderResponse, OrderStatus, ProductStatus
from src.routers.auth import RoleChecker

router = APIRouter()
RATE_LIMIT_MINUTES = 1

allow_users_admins = RoleChecker(["USER", "ADMIN"])


def error_resp(code: int, err_code: str, msg: str, details: dict = None):
    return JSONResponse(status_code=code, content={"error_code": err_code, "message": msg, "details": details})


@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(order_in: OrderCreate, session: AsyncSession = Depends(get_async_session),
                       user: dict = Depends(allow_users_admins)):
    user_id = uuid.UUID(user["sub"])
    now = datetime.now(timezone.utc)

    time_threshold = now - timedelta(minutes=RATE_LIMIT_MINUTES)
    recent_op_query = select(UserOperationDB).where(
        and_(UserOperationDB.user_id == user_id, UserOperationDB.operation_type == 'CREATE_ORDER',
             UserOperationDB.created_at >= time_threshold))
    if (await session.execute(recent_op_query)).scalars().first():
        return error_resp(429, "ORDER_LIMIT_EXCEEDED", "Rate limit exceeded")

    active_order_query = select(OrderDB).where(
        and_(OrderDB.user_id == user_id, OrderDB.status.in_([OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING])))
    if (await session.execute(active_order_query)).scalars().first():
        return error_resp(409, "ORDER_HAS_ACTIVE", "User has active order")

    total_amount = 0
    items_to_create = []
    for item in order_in.items:
        product = (await session.execute(
            select(ProductDB).where(ProductDB.id == uuid.UUID(item.product_id)))).scalars().first()
        if not product: return error_resp(404, "PRODUCT_NOT_FOUND", "Product not found")
        if product.status != ProductStatus.ACTIVE: return error_resp(409, "PRODUCT_INACTIVE", "Product inactive")
        if product.stock < item.quantity: return error_resp(409, "INSUFFICIENT_STOCK", "Not enough stock",
                                                            {"product_id": item.product_id})

        product.stock -= item.quantity
        price_at_order = float(product.price)
        total_amount += price_at_order * item.quantity
        items_to_create.append(
            OrderItemDB(product_id=product.id, quantity=item.quantity, price_at_order=price_at_order))

    discount_amount = 0
    promo_code_id = None
    if order_in.promo_code:
        promo = (
            await session.execute(select(PromoCodeDB).where(PromoCodeDB.code == order_in.promo_code))).scalars().first()
        if not promo or not promo.active or promo.current_uses >= promo.max_uses or not (
                promo.valid_from <= now <= promo.valid_until):
            return error_resp(422, "PROMO_CODE_INVALID", "Promo invalid")
        if total_amount < float(promo.min_order_amount):
            return error_resp(422, "PROMO_CODE_MIN_AMOUNT", "Below minimum amount")

        if promo.discount_type == 'PERCENTAGE':
            discount_amount = total_amount * (float(promo.discount_value) / 100)
            if discount_amount > total_amount * 0.7: discount_amount = 0
        else:
            discount_amount = min(float(promo.discount_value), total_amount)
        promo.current_uses += 1
        promo_code_id = promo.id
        total_amount -= discount_amount

    new_order = OrderDB(user_id=user_id, status=OrderStatus.CREATED, total_amount=total_amount,
                        discount_amount=discount_amount, promo_code_id=promo_code_id)
    session.add(new_order)
    await session.flush()

    for oi in items_to_create:
        oi.order_id = new_order.id
        session.add(oi)

    session.add(UserOperationDB(user_id=user_id, operation_type='CREATE_ORDER'))
    await session.commit()
    return error_resp(201, "CREATED", "Use actual schema mapping in prod", {"id": str(new_order.id)})


@router.post("/{id}/cancel", status_code=status.HTTP_200_OK)
async def cancel_order(id: uuid.UUID = Path(...), session: AsyncSession = Depends(get_async_session),
                       user: dict = Depends(allow_users_admins)):
    user_id = uuid.UUID(user["sub"])
    order = (await session.execute(select(OrderDB).where(OrderDB.id == id))).scalars().first()

    if not order:
        return error_resp(404, "ORDER_NOT_FOUND", "Order not found")
    if user["role"] != "ADMIN" and order.user_id != user_id:
        return error_resp(403, "ORDER_OWNERSHIP_VIOLATION", "Access denied")
    if order.status not in [OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING]:
        return error_resp(409, "INVALID_STATE_TRANSITION", "Invalid state")

    items = (await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == order.id))).scalars().all()
    for item in items:
        product = (await session.execute(select(ProductDB).where(ProductDB.id == item.product_id))).scalars().first()
        if product: product.stock += item.quantity

    if order.promo_code_id:
        promo = (
            await session.execute(select(PromoCodeDB).where(PromoCodeDB.id == order.promo_code_id))).scalars().first()
        if promo: promo.current_uses -= 1

    order.status = OrderStatus.CANCELED
    await session.commit()
    return {"status": "success"}