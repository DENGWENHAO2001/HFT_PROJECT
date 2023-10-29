from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import os

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'database', 'test.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class DataModel(db.Model):
    __tablename__ = 'data'

    id = db.Column(db.Integer, primary_key=True)
    timestep = db.Column(db.String(80), nullable=False)
    position = db.Column(db.Float, nullable=False)
    output_action=db.Column(db.Integer, nullable=False)
    bid1_price=db.Column(db.Float, nullable=False)
    bid1_size=db.Column(db.Float, nullable=False)
    bid2_price=db.Column(db.Float, nullable=False)
    bid2_size=db.Column(db.Float, nullable=False)
    bid3_price=db.Column(db.Float, nullable=False)
    bid3_size=db.Column(db.Float, nullable=False)
    bid4_price=db.Column(db.Float, nullable=False)
    bid4_size=db.Column(db.Float, nullable=False)
    bid5_price=db.Column(db.Float, nullable=False)
    bid5_size=db.Column(db.Float, nullable=False)
    ask1_price=db.Column(db.Float, nullable=False)
    ask1_size=db.Column(db.Float, nullable=False)
    ask2_price=db.Column(db.Float, nullable=False)
    ask2_size=db.Column(db.Float, nullable=False)
    ask3_price=db.Column(db.Float, nullable=False)
    ask3_size=db.Column(db.Float, nullable=False)
    ask4_price=db.Column(db.Float, nullable=False)
    ask4_size=db.Column(db.Float, nullable=False)
    ask5_price=db.Column(db.Float, nullable=False)
    ask5_size=db.Column(db.Float, nullable=False)



@app.route('/store', methods=['POST'])
def store_data():
    data = request.json
    if not data  :
        return jsonify({'error': 'Invalid data'}), 400

    required_fields = ['timestep', 'position', 'output_action', 'bid1_price', 'bid1_size',
                       'bid2_price', 'bid2_size','bid3_price', 'bid3_size', 'bid4_price',
                       'bid4_size', 'bid5_price', 'bid5_size', 'ask1_price','ask1_size',
                       'ask2_price', 'ask2_size', 'ask3_price', 'ask3_size', 'ask4_price',
                       'ask4_size','ask5_price', 'ask5_size']

    for field in required_fields:
        if field not in data or data[field] is None:
            return jsonify({'error': 'Invalid data, {} is missing or None'.format(field)}), 400
        if not isinstance(data['timestep'], str):
            return jsonify({'error': 'Invalid data type of timestep'}), 400
        if not isinstance(data['output_action'], int):
            return jsonify({'error': 'Invalid data type of output_action'}), 400

    max_data_count = 60*60*24*7#second*minute*hours*day
    current_data_count = DataModel.query.count()

    if current_data_count >= max_data_count:
        oldest_data = DataModel.query.order_by(DataModel.id).first()
        db.session.delete(oldest_data)
        db.session.commit()

    new_data = DataModel(**data)
    db.session.add(new_data)
    db.session.commit()
    return jsonify({'message': 'Data stored successfully!'}), 201

@app.route('/get_latest', methods=['GET'])
def get_latest_data():
    latest_data = DataModel.query.order_by(DataModel.id.desc()).first()
    if latest_data is None:
        return jsonify({'error': 'No data available'}), 404
    data_dict = {column.name: getattr(latest_data, column.name) for column in latest_data.__table__.columns}
    return jsonify(data_dict), 200






if __name__ == '__main__':


    host = "127.0.0.1"
    port = 8080
    with app.app_context():
        db.create_all()
    app.run(host=host,port=port,debug=True)
