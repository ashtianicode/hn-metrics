import os
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from rest.extensions import db
from rest.app import app
app.config.from_object(os.environ["APP_SETTINGS"])


mirgrate = Migrate(app,db)
manager = Manager(app)
manager.add_command("db",MigrateCommand)


if __name__ == "__main__":
	manager.run()