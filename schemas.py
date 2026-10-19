from datetime import date
from uuid import UUID
from pydantic import BaseModel, Field, field_serializer

class UserRegisterRequest(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=100, description="Имя пользователя")
    second_name: str = Field(..., min_length=1, max_length=100, description="Фамилия пользователя")
    birthdate: date = Field(..., description="Дата рождения в формате ГГГГ-ММ-ДД")
    gender: str = Field(..., min_length=1, max_length=10, description="Пол пользователя")
    biography: str | None = Field(default=None, description="Интересы или краткая биография")
    city: str = Field(..., min_length=1, max_length=100, description="Город проживания")
    password: str = Field(..., min_length=6, description="Пароль пользователя")

class UserRegisterResponse(BaseModel):
    user_id: str = Field(..., description="Идентификатор созданного пользователя (UUID)")

class LoginRequest(BaseModel):
    id: str = Field(..., description="Идентификатор пользователя (UUID)")
    password: str = Field(..., description="Пароль")

class LoginResponse(BaseModel):
    token: str = Field(..., description="JWT-токен авторизации")

class UserGetResponse(BaseModel):
    id: UUID = Field(..., description="Идентификатор пользователя (UUID)")
    first_name: str = Field(..., description="Имя")
    second_name: str = Field(..., description="Фамилия")
    birthdate: date = Field(..., description="Дата рождения")
    gender: str = Field(..., description="Пол")
    biography: str | None = Field(default=None, description="Интересы/биография")
    city: str = Field(..., description="Город")

    @field_serializer("birthdate")
    def serialize_birthdate(self, birthdate: date) -> str:
        return birthdate.strftime("%Y-%m-%d")

    @field_serializer("id")
    def serialize_uuid(self, id_val: UUID) -> str:
        return str(id_val)


class PostCreateRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Текст поста")


class PostUpdateRequest(BaseModel):
    id: str = Field(..., description="Идентификатор поста (UUID)")
    text: str = Field(..., min_length=1, description="Новый текст поста")


class PostResponse(BaseModel):
    id: str = Field(..., description="Идентификатор поста")
    text: str = Field(..., description="Текст поста")
    author_user_id: str = Field(..., description="Идентификатор автора")


class PostIdResponse(BaseModel):
    id: str = Field(..., description="Идентификатор созданного поста")


# ========================= DIALOGS =========================

class DialogSendRequest(BaseModel):
    """Тело запроса отправки сообщения (POST /dialog/{user_id}/send)."""
    text: str = Field(..., min_length=1, description="Текст сообщения")


class DialogMessageResponse(BaseModel):
    """
    Элемент ответа списка сообщений диалога (GET /dialog/{user_id}/list).

    Поля соответствуют спецификации OpenAPI:
      from — UUID отправителя
      to   — UUID получателя
      text — текст сообщения

    Примечание: поле «from» — зарезервированное слово в Python,
    поэтому используем псевдоним через Field(alias=...) и
    model_config для работы с aliased полями.
    """
    model_config = {"populate_by_name": True}

    from_user: str = Field(..., alias="from", description="UUID отправителя")
    to_user: str = Field(..., alias="to", description="UUID получателя")
    text: str = Field(..., description="Текст сообщения")
