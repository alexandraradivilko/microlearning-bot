class MessageTemplateDto:
    def __init__(self, content, id):
        self._content = content
        self._id = id

    # Геттеры
    def get_content(self):
        return self._content

    def get_id(self):
        return self._id

    # Сеттеры
    def set_content(self, content):
        self._content = content

    def set_id(self, id):
        self._id = id

    def __repr__(self):
        return f"MessageTemplateDto(content={self._content}, id={self._id})"


# Класс MessageTemplateDto должен быть определён отдельно, а не внутри функции

class MessageTemplateDto:
    def __init__(self, content, id):
        self._content = content
        self._id = id

    # Геттеры
    def get_content(self):
        return self._content

    def get_id(self):
        return self._id

    # Сеттеры
    def set_content(self, content):
        self._content = content

    def set_id(self, id):
        self._id = id

    def __repr__(self):
        return f"MessageTemplateDto(content={self._content}, id={self._id})"

