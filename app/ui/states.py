from aiogram.fsm.state import StatesGroup, State


class CardFill(StatesGroup):
    wait_local_name = State()
    wait_photo = State()


class AdminStates(StatesGroup):
    wait_seller_add = State()
    wait_seller_del = State()
    wait_admin_add = State()
    wait_admin_del = State()
    wait_seller_rename_pick = State()
    wait_seller_rename_name = State()


class AdminEdit(StatesGroup):
    # ожидание текста для выбранного поля
    wait_text = State()
    # ожидание фото для замены
    wait_photo = State()


class AdminCreate(StatesGroup):
    # создание нового товара: ожидание артикула
    wait_article = State()
    # универсальный ввод количества после выбора локации
    wait_qty = State()

class RegStates(StatesGroup):
    # Регистрация нового пользователя: сначала имя, затем фамилия
    wait_first_name = State()
    wait_last_name = State()


# ===== Scheduling states =====
class SchedStates(StatesGroup):
    # Viewing calendar, holds current ym
    viewing = State()

class SchedTransfer(StatesGroup):
    # After user clicks a date, we show available employees to "поменяться"
    wait_target_for_date = State()

class SchedAdmin(StatesGroup):
    # Create anchor: pick first date, then two employees, then next date, then two employees
    anchor_pick_date1 = State()
    anchor_pick_two_for_date1 = State()
    anchor_pick_two_for_date2 = State()
    # Swap globally input
    swap_global_pick_a = State()
    swap_global_pick_b = State()
