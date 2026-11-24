package monedas.api.aplicacion.concurrencia;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Componente encargado de contabilizar los intentos de inicio de sesión
 * en {@code UsuarioServicio.login(...)} mediante tres variantes de control
 * de concurrencia para análisis experimental:
 * <ul>
 *   <li><b>Variante A (SIN_CONTROL):</b> Campo primitivo {@code long} sin sincronización ({@code contador++}).</li>
 *   <li><b>Variante B (SINCRONIZADO):</b> Exclusión mutua mediante bloque {@code synchronized}.</li>
 *   <li><b>Variante C (ATOMICO):</b> Operación atómica sin bloqueos (lock-free) usando {@link AtomicLong}.</li>
 * </ul>
 */
@Component
public class ContadorIntentosLogin {

    public enum Variante {
        SIN_CONTROL,
        SINCRONIZADO,
        ATOMICO
    }

    // Variante A: variable primitiva sin control de concurrencia
    private long intentosSinControl = 0L;

    // Variante B: exclusión mutua mediante monitor explícito (synchronized)
    private long intentosSincronizado = 0L;
    private final Object monitor = new Object();

    // Variante C: operación atómica lock-free (CAS), análoga a CubetaTokens
    private final AtomicLong intentosAtomico = new AtomicLong(0L);

    // Por defecto en producción se utiliza la variante segura lock-free
    private volatile Variante varianteActiva = Variante.ATOMICO;

    public Variante getVarianteActiva() {
        return varianteActiva;
    }

    public void setVarianteActiva(Variante varianteActiva) {
        if (varianteActiva == null) {
            throw new IllegalArgumentException("La variante de contador no puede ser nula");
        }
        this.varianteActiva = varianteActiva;
    }

    /**
     * Registra un intento de login utilizando la variante actualmente configurada.
     */
    public void registrarIntento() {
        switch (varianteActiva) {
            case SIN_CONTROL -> registrarIntentoSinControl();
            case SINCRONIZADO -> registrarIntentoSincronizado();
            case ATOMICO -> registrarIntentoAtomico();
        }
    }

    // --- Variante A: Sin control de concurrencia ---

    public void registrarIntentoSinControl() {
        intentosSinControl++;
    }

    public long getIntentosSinControl() {
        return intentosSinControl;
    }

    // --- Variante B: Sincronización con cerrojo (synchronized) ---

    public void registrarIntentoSincronizado() {
        synchronized (monitor) {
            intentosSincronizado++;
        }
    }

    public long getIntentosSincronizado() {
        synchronized (monitor) {
            return intentosSincronizado;
        }
    }

    // --- Variante C: Operación atómica (AtomicLong) ---

    public void registrarIntentoAtomico() {
        intentosAtomico.incrementAndGet();
    }

    public long getIntentosAtomico() {
        return intentosAtomico.get();
    }

    /**
     * Obtiene el valor actual del contador correspondiente a la variante indicada.
     */
    public long obtenerConteo(Variante variante) {
        return switch (variante) {
            case SIN_CONTROL -> getIntentosSinControl();
            case SINCRONIZADO -> getIntentosSincronizado();
            case ATOMICO -> getIntentosAtomico();
        };
    }

    /**
     * Reinicia todos los contadores a cero para aislar cada corrida experimental.
     */
    public void reiniciar() {
        intentosSinControl = 0L;
        synchronized (monitor) {
            intentosSincronizado = 0L;
        }
        intentosAtomico.set(0L);
    }
}
