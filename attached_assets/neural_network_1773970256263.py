"""
████████████████████████████████████████████████████████████████████████████████
█                                                                              █
█  ★ KR √¡\ K - NEURAL NETWORK MODULE                                         █
█  LSTM + Attention + Transformer for Price Prediction                        █
█                                                                              █
████████████████████████████████████████████████████████████████████████████████
"""

import os
import json
import time
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
from pathlib import Path

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger('KRAKEN-NEURAL')

@dataclass
class NeuralConfig:
    """Neural network configuration"""
    input_size: int = 10
    hidden_size: int = 64
    num_layers: int = 2
    output_size: int = 3
    dropout: float = 0.2
    learning_rate: float = 0.001
    sequence_length: int = 60
    batch_size: int = 32
    epochs: int = 50
    device: str = "cpu"


class AttentionLayer(nn.Module):
    """Self-attention mechanism for sequence data"""
    
    def __init__(self, hidden_size: int):
        super().__init__()
        self.attention = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.Tanh(),
            nn.Linear(hidden_size, 1)
        )
    
    def forward(self, lstm_output: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        attention_weights = torch.softmax(self.attention(lstm_output), dim=1)
        context_vector = torch.sum(attention_weights * lstm_output, dim=1)
        return context_vector, attention_weights


class LSTMPricePredictor(nn.Module):
    """LSTM with Attention for price direction prediction"""
    
    def __init__(self, config: NeuralConfig):
        super().__init__()
        self.config = config
        
        self.lstm = nn.LSTM(
            input_size=config.input_size,
            hidden_size=config.hidden_size,
            num_layers=config.num_layers,
            batch_first=True,
            dropout=config.dropout if config.num_layers > 1 else 0
        )
        
        self.attention = AttentionLayer(config.hidden_size)
        
        self.fc = nn.Sequential(
            nn.Linear(config.hidden_size, config.hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_size // 2, config.output_size)
        )
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        lstm_out, _ = self.lstm(x)
        context, attention_weights = self.attention(lstm_out)
        output = self.fc(context)
        return output, attention_weights


class TransformerPredictor(nn.Module):
    """Transformer-based price predictor"""
    
    def __init__(self, config: NeuralConfig):
        super().__init__()
        self.config = config
        
        self.input_projection = nn.Linear(config.input_size, config.hidden_size)
        
        self.positional_encoding = nn.Parameter(
            torch.randn(1, config.sequence_length, config.hidden_size)
        )
        
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=config.hidden_size,
            nhead=4,
            dim_feedforward=config.hidden_size * 4,
            dropout=config.dropout,
            batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=config.num_layers)
        
        self.fc = nn.Sequential(
            nn.Linear(config.hidden_size, config.hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_size // 2, config.output_size)
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.input_projection(x)
        x = x + self.positional_encoding[:, :x.size(1), :]
        x = self.transformer(x)
        x = x[:, -1, :]
        return self.fc(x)


class EnsemblePredictor(nn.Module):
    """Ensemble of LSTM and Transformer"""
    
    def __init__(self, config: NeuralConfig):
        super().__init__()
        self.lstm = LSTMPricePredictor(config)
        self.transformer = TransformerPredictor(config)
        self.weight = nn.Parameter(torch.tensor([0.5]))
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        lstm_out, _ = self.lstm(x)
        transformer_out = self.transformer(x)
        weight = torch.sigmoid(self.weight)
        return weight * lstm_out + (1 - weight) * transformer_out


class PriceDataset(Dataset):
    """Dataset for price sequences"""
    
    def __init__(self, features: np.ndarray, labels: np.ndarray):
        self.features = torch.FloatTensor(features)
        self.labels = torch.LongTensor(labels)
    
    def __len__(self):
        return len(self.features)
    
    def __getitem__(self, idx):
        return self.features[idx], self.labels[idx]


class FeatureExtractor:
    """Extract trading features from price data"""
    
    def __init__(self):
        self.scaler = MinMaxScaler()
        self.fitted = False
    
    def compute_features(self, prices: List[float], volumes: List[float] = None) -> np.ndarray:
        """Compute technical indicators as features"""
        prices = np.array(prices)
        n = len(prices)
        
        if n < 20:
            return None
        
        features = []
        
        returns = np.diff(prices) / prices[:-1]
        returns = np.concatenate([[0], returns])
        features.append(returns)
        
        for window in [5, 10, 20]:
            if n >= window:
                sma = np.convolve(prices, np.ones(window)/window, mode='same')
                features.append((prices - sma) / (sma + 1e-8))
        
        for window in [5, 10, 20]:
            if n >= window:
                volatility = np.array([np.std(prices[max(0,i-window):i+1]) for i in range(n)])
                features.append(volatility / (np.mean(volatility) + 1e-8))
        
        momentum_5 = np.zeros(n)
        momentum_10 = np.zeros(n)
        for i in range(5, n):
            momentum_5[i] = (prices[i] - prices[i-5]) / (prices[i-5] + 1e-8)
        for i in range(10, n):
            momentum_10[i] = (prices[i] - prices[i-10]) / (prices[i-10] + 1e-8)
        features.append(momentum_5)
        features.append(momentum_10)
        
        delta = np.diff(prices, prepend=prices[0])
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = np.convolve(gain, np.ones(14)/14, mode='same')
        avg_loss = np.convolve(loss, np.ones(14)/14, mode='same')
        rs = avg_gain / (avg_loss + 1e-8)
        rsi = 100 - (100 / (1 + rs))
        features.append(rsi / 100)
        
        return np.column_stack(features)
    
    def fit_transform(self, features: np.ndarray) -> np.ndarray:
        """Fit scaler and transform"""
        self.fitted = True
        return self.scaler.fit_transform(features)
    
    def transform(self, features: np.ndarray) -> np.ndarray:
        """Transform using fitted scaler"""
        if not self.fitted:
            return self.fit_transform(features)
        return self.scaler.transform(features)


class NeuralSignalGenerator:
    """Neural network-based signal generator for trading"""
    
    def __init__(self, pair: str, config: NeuralConfig = None):
        self.pair = pair
        self.config = config or NeuralConfig()
        self.logger = logging.getLogger(f'NEURAL-{pair}')
        
        self.model = None
        self.feature_extractor = FeatureExtractor()
        self.price_history = deque(maxlen=500)
        self.volume_history = deque(maxlen=500)
        
        self.is_trained = False
        self.last_prediction = None
        self.confidence = 0.0
        self.prediction_history = deque(maxlen=100)
        
        self.model_path = Path(f"neural_models/{pair}_model.pt")
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._initialize_model()
        self._load_model()
    
    def _initialize_model(self):
        """Initialize the neural network model"""
        self.model = EnsemblePredictor(self.config)
        self.model.to(self.config.device)
        self.optimizer = optim.Adam(self.model.parameters(), lr=self.config.learning_rate)
        self.criterion = nn.CrossEntropyLoss()
    
    def _load_model(self):
        """Load saved model if exists"""
        if self.model_path.exists():
            try:
                checkpoint = torch.load(self.model_path, map_location=self.config.device)
                self.model.load_state_dict(checkpoint['model_state'])
                self.optimizer.load_state_dict(checkpoint['optimizer_state'])
                self.is_trained = checkpoint.get('is_trained', True)
                self.logger.info(f"Loaded neural model for {self.pair}")
            except Exception as e:
                self.logger.warning(f"Could not load model: {e}")
    
    def _save_model(self):
        """Save model checkpoint"""
        try:
            torch.save({
                'model_state': self.model.state_dict(),
                'optimizer_state': self.optimizer.state_dict(),
                'is_trained': self.is_trained
            }, self.model_path)
        except Exception as e:
            self.logger.warning(f"Could not save model: {e}")
    
    def update(self, price: float, volume: float = 0.0):
        """Update with new price data"""
        self.price_history.append(price)
        self.volume_history.append(volume)
    
    def _prepare_sequences(self, features: np.ndarray, lookahead: int = 5) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare sequences for training"""
        n = len(features)
        seq_len = self.config.sequence_length
        
        if n < seq_len + lookahead:
            return None, None
        
        X, y = [], []
        prices = list(self.price_history)
        
        for i in range(seq_len, n - lookahead):
            X.append(features[i-seq_len:i])
            future_return = (prices[i + lookahead] - prices[i]) / prices[i]
            
            if future_return > 0.005:
                label = 2
            elif future_return < -0.005:
                label = 0
            else:
                label = 1
            y.append(label)
        
        return np.array(X), np.array(y)
    
    def train(self, epochs: int = None) -> Dict:
        """Train the neural network on historical data"""
        epochs = epochs or self.config.epochs
        
        if len(self.price_history) < self.config.sequence_length + 50:
            return {'status': 'insufficient_data', 'samples': len(self.price_history)}
        
        prices = list(self.price_history)
        volumes = list(self.volume_history)
        
        features = self.feature_extractor.compute_features(prices, volumes)
        if features is None:
            return {'status': 'feature_extraction_failed'}
        
        features = self.feature_extractor.fit_transform(features)
        
        X, y = self._prepare_sequences(features)
        if X is None:
            return {'status': 'sequence_preparation_failed'}
        
        split = int(len(X) * 0.8)
        train_dataset = PriceDataset(X[:split], y[:split])
        val_dataset = PriceDataset(X[split:], y[split:])
        
        train_loader = DataLoader(train_dataset, batch_size=self.config.batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=self.config.batch_size)
        
        self.model.train()
        best_val_loss = float('inf')
        
        for epoch in range(epochs):
            train_loss = 0.0
            for batch_x, batch_y in train_loader:
                batch_x = batch_x.to(self.config.device)
                batch_y = batch_y.to(self.config.device)
                
                self.optimizer.zero_grad()
                outputs = self.model(batch_x)
                loss = self.criterion(outputs, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                self.optimizer.step()
                
                train_loss += loss.item()
            
            self.model.eval()
            val_loss = 0.0
            correct = 0
            total = 0
            
            with torch.no_grad():
                for batch_x, batch_y in val_loader:
                    batch_x = batch_x.to(self.config.device)
                    batch_y = batch_y.to(self.config.device)
                    
                    outputs = self.model(batch_x)
                    val_loss += self.criterion(outputs, batch_y).item()
                    
                    _, predicted = torch.max(outputs, 1)
                    total += batch_y.size(0)
                    correct += (predicted == batch_y).sum().item()
            
            val_accuracy = correct / total if total > 0 else 0
            
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                self._save_model()
            
            self.model.train()
        
        self.is_trained = True
        self._save_model()
        
        return {
            'status': 'trained',
            'epochs': epochs,
            'train_samples': len(train_dataset),
            'val_samples': len(val_dataset),
            'val_accuracy': val_accuracy,
            'val_loss': best_val_loss
        }
    
    def predict(self) -> Tuple[str, float]:
        """Predict price direction with confidence"""
        if len(self.price_history) < self.config.sequence_length + 20:
            return 'hold', 0.0
        
        prices = list(self.price_history)
        volumes = list(self.volume_history)
        
        features = self.feature_extractor.compute_features(prices, volumes)
        if features is None:
            return 'hold', 0.0
        
        features = self.feature_extractor.transform(features)
        
        sequence = features[-self.config.sequence_length:]
        sequence = torch.FloatTensor(sequence).unsqueeze(0).to(self.config.device)
        
        self.model.eval()
        with torch.no_grad():
            output = self.model(sequence)
            probabilities = torch.softmax(output, dim=1)
            confidence, predicted = torch.max(probabilities, 1)
        
        prediction_map = {0: 'sell', 1: 'hold', 2: 'buy'}
        prediction = prediction_map[predicted.item()]
        confidence_score = confidence.item()
        
        self.last_prediction = prediction
        self.confidence = confidence_score
        self.prediction_history.append({
            'time': time.time(),
            'prediction': prediction,
            'confidence': confidence_score,
            'price': prices[-1]
        })
        
        return prediction, confidence_score
    
    def get_signal(self, min_confidence: float = 0.6) -> Optional[str]:
        """Get trading signal if confidence is high enough"""
        prediction, confidence = self.predict()
        
        if confidence >= min_confidence and prediction != 'hold':
            return prediction
        return None
    
    def get_stats(self) -> Dict:
        """Get neural network statistics"""
        return {
            'pair': self.pair,
            'is_trained': self.is_trained,
            'data_points': len(self.price_history),
            'last_prediction': self.last_prediction,
            'confidence': self.confidence,
            'predictions_made': len(self.prediction_history)
        }


class NeuralNetworkManager:
    """Manages neural networks for all trading pairs"""
    
    def __init__(self, pairs: List[str], config: NeuralConfig = None):
        self.config = config or NeuralConfig()
        self.logger = logging.getLogger('KRAKEN-NEURAL-MGR')
        self.generators = {}
        
        for pair in pairs:
            self.generators[pair] = NeuralSignalGenerator(pair, self.config)
        
        self.logger.info(f"Neural Network Manager initialized for {len(pairs)} pairs")
    
    def update(self, pair: str, price: float, volume: float = 0.0):
        """Update a specific pair with new data"""
        if pair in self.generators:
            self.generators[pair].update(price, volume)
    
    def get_signal(self, pair: str, min_confidence: float = 0.6) -> Optional[str]:
        """Get signal for a specific pair"""
        if pair in self.generators:
            return self.generators[pair].get_signal(min_confidence)
        return None
    
    def train_all(self, epochs: int = 50) -> Dict:
        """Train all neural networks"""
        results = {}
        for pair, generator in self.generators.items():
            self.logger.info(f"Training neural network for {pair}...")
            results[pair] = generator.train(epochs)
        return results
    
    def get_all_stats(self) -> Dict:
        """Get stats for all pairs"""
        return {pair: gen.get_stats() for pair, gen in self.generators.items()}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    config = NeuralConfig(
        input_size=10,
        hidden_size=64,
        num_layers=2,
        sequence_length=60
    )
    
    generator = NeuralSignalGenerator("PF_XBTUSD", config)
    
    np.random.seed(42)
    base_price = 40000
    for i in range(500):
        noise = np.random.normal(0, 100)
        trend = 10 * np.sin(i / 50)
        price = base_price + trend + noise
        generator.update(price, np.random.uniform(100, 1000))
    
    print("Training neural network...")
    result = generator.train(epochs=10)
    print(f"Training result: {result}")
    
    prediction, confidence = generator.predict()
    print(f"Prediction: {prediction}, Confidence: {confidence:.2%}")
    
    signal = generator.get_signal(min_confidence=0.5)
    print(f"Trading signal: {signal}")
