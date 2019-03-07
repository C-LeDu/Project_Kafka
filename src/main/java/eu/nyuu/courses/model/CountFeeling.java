package eu.nyuu.courses.model;

public class CountFeeling {
    private Long negatif;
    private Long positif;
    private Long neutre;

    public CountFeeling() {
        this.negatif = 0L;
        this.positif = 0L;
        this.neutre = 0L;
    }

    public void addNegatif(){
        this.negatif += 1;
    }
    public void addPosiftif(){
        this.positif += 1;
    }
    public void addNeutre(){
        this.neutre += 1;
    }

    public void addNegatif(long add){
        this.negatif += add;
    }
    public void addPosiftif(long add){
        this.positif += add;
    }
    public void addNeutre(long add){
        this.neutre += add;
    }



    public Long getNegatif() {
        return negatif;
    }

    public void setNegatif(Long negatif) {
        this.negatif = negatif;
    }

    public Long getPositif() {
        return positif;
    }

    public void setPositif(Long positif) {
        this.positif = positif;
    }

    public Long getNeutre() {
        return neutre;
    }

    public void setNeutre(Long neutre) {
        this.neutre = neutre;
    }
}
