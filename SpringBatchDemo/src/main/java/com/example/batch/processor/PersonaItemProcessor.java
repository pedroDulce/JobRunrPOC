package com.example.batch.processor;

import com.example.batch.model.Persona;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;

public class PersonaItemProcessor implements ItemProcessor<Persona, Persona> {

    @Override
    public Persona process(@NonNull Persona persona) throws Exception {
         Thread.sleep(1000); 
        persona.setNombre(persona.getNombre().toUpperCase());
        return persona;
    }
}
