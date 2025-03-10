from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

class Strategy(ABC):
    """Abstract base class for trading strategies."""
    
    @abstractmethod
    def initialize(self, parameters: Dict[str, Any]) -> None:
        """Initialize strategy with parameters.
        
        Args:
            parameters: Dictionary of strategy parameters
        """
        pass
    
    @abstractmethod
    def on_bar(self, bar_data: pd.DataFrame) -> pd.DataFrame:
        """Process new bar data and generate signals.
        
        Args:
            bar_data: DataFrame containing bar data
            
        Returns:
            pd.DataFrame: DataFrame containing trading signals
        """
        pass

class MovingAverageCrossover(Strategy):
    """Moving Average Crossover strategy implementation."""
    
    def initialize(self, parameters: Dict[str, Any]) -> None:
        """Initialize strategy parameters.
        
        Args:
            parameters: Dictionary containing strategy parameters
                - short_window: Period for short moving average
                - long_window: Period for long moving average
        """
        self.short_window = parameters.get('short_window', 10)
        self.long_window = parameters.get('long_window', 50)
        self.position = 0
        self.signals = []
    
    def on_bar(self, bar_data: pd.DataFrame) -> pd.DataFrame:
        """Process bar data and generate trading signals.
        
        Args:
            bar_data: DataFrame containing bar data with at least 'close' column
            
        Returns:
            pd.DataFrame: DataFrame containing trading signals
        """
        # Calculate moving averages
        bar_data['short_ma'] = bar_data['close'].rolling(window=self.short_window).mean()
        bar_data['long_ma'] = bar_data['close'].rolling(window=self.long_window).mean()
        
        # Generate signals
        bar_data['signal'] = 0.0
        bar_data['signal'][self.short_window:] = np.where(
            bar_data['short_ma'][self.short_window:] > bar_data['long_ma'][self.short_window:], 1.0, 0.0
        )
        bar_data['position'] = bar_data['signal'].diff()
        
        # Filter actionable signals
        signals = bar_data[bar_data['position'] != 0].copy()
        signals['side'] = np.where(signals['position'] > 0, 'buy', 'sell')
        signals['quantity'] = 100  # Fixed quantity for simplicity
        
        return signals

class RSIStrategy(Strategy):
    """Relative Strength Index (RSI) strategy implementation."""
    
    def initialize(self, parameters: Dict[str, Any]) -> None:
        """Initialize strategy parameters.
        
        Args:
            parameters: Dictionary containing strategy parameters
                - rsi_period: Period for RSI calculation
                - overbought_level: RSI level for overbought condition
                - oversold_level: RSI level for oversold condition
        """
        self.rsi_period = parameters.get('rsi_period', 14)
        self.overbought_level = parameters.get('overbought_level', 70)
        self.oversold_level = parameters.get('oversold_level', 30)
        self.position = 0
    
    def calculate_rsi(self, data: pd.Series) -> pd.Series:
        """Calculate Relative Strength Index.
        
        Args:
            data: Price series
            
        Returns:
            pd.Series: RSI values
        """
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def on_bar(self, bar_data: pd.DataFrame) -> pd.DataFrame:
        """Process bar data and generate trading signals.
        
        Args:
            bar_data: DataFrame containing bar data with at least 'close' column
            
        Returns:
            pd.DataFrame: DataFrame containing trading signals
        """
        # Calculate RSI
        bar_data['rsi'] = self.calculate_rsi(bar_data['close'])
        
        # Generate signals
        bar_data['signal'] = 0.0
        bar_data.loc[bar_data['rsi'] < self.oversold_level, 'signal'] = 1.0  # Buy signal
        bar_data.loc[bar_data['rsi'] > self.overbought_level, 'signal'] = -1.0  # Sell signal
        
        # Filter actionable signals
        signals = bar_data[bar_data['signal'] != 0].copy()
        signals['side'] = np.where(signals['signal'] > 0, 'buy', 'sell')
        signals['quantity'] = 100  # Fixed quantity for simplicity
        
        return signals

class BollingerBandsStrategy(Strategy):
    """Bollinger Bands strategy implementation."""
    
    def initialize(self, parameters: Dict[str, Any]) -> None:
        """Initialize strategy parameters.
        
        Args:
            parameters: Dictionary containing strategy parameters
                - window: Period for moving average calculation
                - num_std: Number of standard deviations for bands
        """
        self.window = parameters.get('window', 20)
        self.num_std = parameters.get('num_std', 2)
        self.position = 0
    
    def calculate_bollinger_bands(self, data: pd.Series) -> tuple:
        """Calculate Bollinger Bands.
        
        Args:
            data: Price series
            
        Returns:
            tuple: (middle band, upper band, lower band)
        """
        middle_band = data.rolling(window=self.window).mean()
        std = data.rolling(window=self.window).std()
        upper_band = middle_band + (std * self.num_std)
        lower_band = middle_band - (std * self.num_std)
        
        return middle_band, upper_band, lower_band
    
    def on_bar(self, bar_data: pd.DataFrame) -> pd.DataFrame:
        """Process bar data and generate trading signals.
        
        Args:
            bar_data: DataFrame containing bar data with at least 'close' column
            
        Returns:
            pd.DataFrame: DataFrame containing trading signals
        """
        # Calculate Bollinger Bands
        middle, upper, lower = self.calculate_bollinger_bands(bar_data['close'])
        bar_data['middle_band'] = middle
        bar_data['upper_band'] = upper
        bar_data['lower_band'] = lower
        
        # Generate signals
        bar_data['signal'] = 0.0
        bar_data.loc[bar_data['close'] < bar_data['lower_band'], 'signal'] = 1.0  # Buy signal
        bar_data.loc[bar_data['close'] > bar_data['upper_band'], 'signal'] = -1.0  # Sell signal
        
        # Filter actionable signals
        signals = bar_data[bar_data['signal'] != 0].copy()
        signals['side'] = np.where(signals['signal'] > 0, 'buy', 'sell')
        signals['quantity'] = 100  # Fixed quantity for simplicity
        
        return signals 