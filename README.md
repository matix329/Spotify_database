# Spotify database

Skrypt `import_album.py` został zaprojektowany do odbierania danych albumów muzycznych z Kafki i zapisywania ich do bazy danych SQLite. Poniżej znajduje się szczegółowa dokumentacja tabel i danych, które są obsługiwane przez ten skrypt.

#### Tabele w SQLite

##### Tabela `Artist`

**Opis**: Przechowuje informacje o artystach muzycznych.

**Kolumny**:
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT): Unikalny identyfikator artysty.
- `name` (TEXT NOT NULL): Nazwa artysty.
- `genre` (TEXT NOT NULL): Gatunek muzyczny artysty (obecnie pole nie jest wypełniane, ale może być rozszerzone w przyszłości).

##### Tabela `album`

**Opis**: Przechowuje informacje o albumach muzycznych.

**Kolumny**:
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT): Unikalny identyfikator albumu.
- `album_name` (TEXT NOT NULL): Nazwa albumu.
- `pub_date` (TEXT NOT NULL): Data wydania albumu.
- `artist_id` (INTEGER NOT NULL, FOREIGN KEY): Identyfikator artysty, który stworzył album. Odwołuje się do kolumny `id` w tabeli `Artist`.

##### Tabela `track`

**Opis**: Przechowuje informacje o utworach muzycznych.

**Kolumny**:
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT): Unikalny identyfikator utworu.
- `title` (TEXT NOT NULL): Tytuł utworu.
- `duration` (INTEGER NOT NULL): Długość utworu w milisekundach.
- `album_id` (INTEGER NOT NULL, FOREIGN KEY): Identyfikator albumu, do którego należy utwór. Odwołuje się do kolumny `id` w tabeli `album`.
