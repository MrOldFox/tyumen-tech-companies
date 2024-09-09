from unittest import mock
import pytest

from src.data_processing.file_processing import process_json_file, is_file_new


@pytest.mark.asyncio
async def test_is_file_stale_file_not_exists():
    """Тест проверяет случай, когда файл не существует."""
    # Подменяем функцию os.path.exists, чтобы она всегда возвращала False.
    # Как будто файла нет.
    with mock.patch('os.path.exists', return_value=False):
        # Вызываем функцию, передавая фиктивный путь к файлу
        result = await is_file_new('dummy_path')
        # Проверяем, что результат True, потому что файла нет
        assert result == True


def test_process_json_file_dask_valid_companies(mocker):
    """
    Тест проверяет корректную фильтрацию компаний по ОКВЭД и региональному коду.
    """
    # Мокаем айтемы чтобы вернуть данные JSON
    # Здесь три компании, но только одна из них должна пройти фильтр (ООО Вася).
    mocker.patch('ijson.items', return_value=[
        {
            'inn': '1234567890',
            'name': 'ООО Вася',
            'kpp': '987654321',
            'data': {
                'СвОКВЭДОсн': {'КодОКВЭД': '62.01'},
                'СвАдресЮЛ': {'АдресРФ': {'КодРегион': '72'}}
            }
        },
        {
            'inn': '9876543210',
            'name': 'ООО Петя',
            'kpp': '123456789',
            'data': {
                'СвОКВЭДОсн': {'КодОКВЭД': '63.01'},
                'СвАдресЮЛ': {'АдресРФ': {'КодРегион': '72'}}
            }
        },
        {
            'inn': '5678901234',
            'name': 'ООО Олег',
            'kpp': '234567891',
            'data': {
                'СвОКВЭДОсн': {'КодОКВЭД': '62.02'},
                'СвАдресЮЛ': {'АдресРФ': {'КодРегион': '71'}}
            }
        }
    ])

    # Создаем фиктивный файл
    json_file = mocker.Mock()

    # Вызываем функцию и превращаем результат в список
    result = list(process_json_file(json_file))

    # Проверяем, что функция нашла только одну компанию (ООО Вася)
    assert len(result) == 1
    assert result[0][0]['inn'] == '1234567890'
    assert result[0][0]['name'] == 'ООО Вася'
    assert '62' in result[0][0]['okved']
    assert result[0][0]['address'] != ''