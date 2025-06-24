from flask import Flask, jsonify, request
from flask_mysqldb import MySQL
from flask_cors import CORS
import pymysql
from datetime import datetime
# Routes
from .routes import CustomersRoutes, CustomersTransaccionRoutes, ProvidersRoutes, ProvidersTransaccionRoutes, GastosRoutes, ContabilidadRoutes, SaldoTarjetaRoutes, SaldoClients, filterDashboardRoutes, SaldoProviders, grafic_gananciasRoutes, grafic_gastosRoutes



def init_app(config):



    app = Flask(__name__)
    app.config.from_object(config)
    CORS(app, origins=["http://localhost:5500"], supports_credentials=True)

    # CORS correctamente aplicado a esta instancia
    # CORS(app, resources={r"/CARNESSHOP/*": {"origins": "*"}}, supports_credentials=True)
    # Blueprints
    app.register_blueprint(CustomersRoutes.main, url_prefix='/CARNESSHOP/customers')
    app.register_blueprint(CustomersTransaccionRoutes.main, url_prefix='/CARNESSHOP/customers/transaccion')
    app.register_blueprint(ProvidersRoutes.main, url_prefix='/CARNESSHOP/provedores')
    app.register_blueprint(ProvidersTransaccionRoutes.main, url_prefix='/CARNESSHOP/providers/transaccion')
    app.register_blueprint(GastosRoutes.main, url_prefix='/CARNESSHOP/gastos')
    app.register_blueprint(ContabilidadRoutes.main, url_prefix='/CARNESSHOP/contabilidad')
    app.register_blueprint(SaldoTarjetaRoutes.main, url_prefix='/CARNESSHOP/cuenta_tarjeta')
    app.register_blueprint(SaldoClients.main, url_prefix='/CARNESSHOP/saldo_cli')
    app.register_blueprint(SaldoProviders.main, url_prefix='/CARNESSHOP/saldo_prov')
    app.register_blueprint(filterDashboardRoutes.main, url_prefix='/CARNESSHOP/filter_Dashboard')
    app.register_blueprint(grafic_gananciasRoutes.main, url_prefix='/CARNESSHOP/graficoGanancias')
    app.register_blueprint(grafic_gastosRoutes.main, url_prefix='/CARNESSHOP/graficoGastos')

    return app
