package com.cldcvr.camouflage.core.failure.strategy;

public interface FailureStrategy {
     void onFailure();

     interface Builder<FailureStrategy>
     {

     }
}
